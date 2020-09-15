from __future__ import print_function

import json
import logging
import os
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory
import csv

from airflow import models
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from resources.stages.raw.schemas_postgres.block import Block
from resources.stages.raw.schemas_postgres.log import Log
from resources.stages.raw.schemas_postgres.receipt import Receipt
from resources.stages.raw.schemas_postgres.transaction import Transaction
from resources.stages.raw.schemas_postgres.base import Base

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

MEGABYTE = 1024 * 1024


def build_load_dag_postgres(
    dag_id,
    cloud_provider,
    output_bucket,
    chain="icon",
    notification_emails=None,
    start_date=datetime(2018, 1, 24),
    schedule_interval="0 0 * * *",
):
    default_dag_args = {
        "depends_on_past": False,
        "start_date": start_date,
        "email_on_failure": True,
        "email_on_retry": True,
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
    }

    if notification_emails and len(notification_emails) > 0:
        default_dag_args["email"] = [
            email.strip() for email in notification_emails.split(",")
        ]

    # Define a DAG (directed acyclic graph) of tasks.
    dag = models.DAG(
        dag_id=dag_id,
        # Daily at 1:30am
        schedule_interval=schedule_interval,
        default_args=default_dag_args,
    )

    if cloud_provider == "aws":
        from airflow.hooks.S3_hook import S3Hook

        cloud_storage_hook = S3Hook(aws_conn_id="aws_default")
    if cloud_provider == "gcp":
        from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

        cloud_storage_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id="google_cloud_default"
        )

    def export_path(directory, date):
        return "export/{directory}/block_date={block_date}/".format(
            directory=directory, block_date=date
        )

    def copy_from_export_path(export_path, file_path):
        logging.info(
            "Calling copy_from_export_path({}, {})".format(export_path, file_path)
        )
        filename = os.path.basename(file_path)
        if cloud_provider == "aws":
            # boto3.s3.Object
            s3_object = cloud_storage_hook.get_key(
                bucket_name=output_bucket, key=export_path + filename
            )
            s3_object.download_file(file_path)
        if cloud_provider == "gcp":
            download_from_gcs(
                bucket=output_bucket, object=export_path + filename, filename=file_path
            )

    def download_from_gcs(bucket, object, filename):
        from google.cloud import storage

        storage_client = storage.Client()

        bucket = storage_client.get_bucket(bucket)
        blob_meta = bucket.get_blob(object)

        if blob_meta.size > 10 * MEGABYTE:
            blob = bucket.blob(object, chunk_size=10 * MEGABYTE)
        else:
            blob = bucket.blob(object)

        blob.download_to_filename(filename)

    def add_load_tasks(task, file_format):
        load_operator = PythonOperator(
            task_id="bucket_to_postgres_{task}".format(task=task),
            dag=dag,
            python_callable=load_task,
            provide_context=True,
            op_kwargs={"conn_id": "postgres", "file_format": file_format, "task": task},
        )
        return load_operator

    def insert_blocks(file, engine, session):
        with open(file) as f:
            reader = csv.reader(f)
            next(reader)

            for line in reader:
                session.add(
                    Block(
                        number=line[0],
                        hash=line[1],
                        parent_hash=line[2],
                        merkle_root_hash=line[3],
                        timestamp=line[4],
                        version=line[5],
                        peer_id=line[6],
                        signature=line[7],
                        next_leader=line[8],
                    )
                )
        session.commit()

    def insert_transactions(file, engine, session):
        with open(file) as f:
            reader = csv.reader(f)
            next(reader)

            for line in reader:
                if line[4] == "":
                    line[4] = None
                if line[5] == "":
                    line[5] = 0
                if isinstance(line[5], str):
                    line[5] = int(line[5], 16)
                if line[6] == "":
                    line[6] = None
                if line[7] == "":
                    line[7] = None
                if line[12] == "":
                    line[12] = None

                session.add(
                    Transaction(
                        version=line[0],
                        from_address=line[1],
                        to_address=line[2],
                        value=line[3],
                        step_limit=line[4],
                        timestamp=line[5],
                        nid=line[6],
                        nonce=line[7],
                        hash=line[8],
                        transaction_index=line[9],
                        block_hash=line[10],
                        block_number=line[11],
                        fee=line[12],
                        signature=line[13],
                        data_type=line[14],
                        data=line[15],
                    )
                )
        session.commit()

    def insert_receipts(file, engine, session):
        with open(file) as f:
            reader = csv.reader(f)
            next(reader)

            for line in reader:
                session.add(
                    Receipt(
                        transaction_hash=line[0],
                        transaction_index=line[1],
                        block_hash=line[2],
                        block_number=line[3],
                        cumulative_step_used=line[4],
                        step_used=line[5],
                        score_address=line[6],
                        status=line[7],
                    )
                )
        session.commit()

    def insert_logs(file, engine, session):
        with open(file) as f:
            logs = json.load(f)
            for log in logs:
                session.add(
                    Log(
                        log_index=log["log_index"],
                        transaction_hash=log["transaction_hash"],
                        transaction_index=log["transaction_index"],
                        block_hash=log["block_hash"],
                        block_number=log["block_number"],
                        address=log["address"],
                        data=log["data"],
                        indexed=log["indexed"],
                    )
                )
        session.commit()

    def load_task(ds, **kwargs):
        conn_id = kwargs.get("conn_id")
        file_format = kwargs.get("file_format")
        task = kwargs.get("task")

        pg_hook = PostgresHook(conn_id)
        engine = pg_hook.get_sqlalchemy_engine()
        Base.metadata.create_all(engine, checkfirst=True)
        Session = sessionmaker(bind=engine)
        session = Session()

        with TemporaryDirectory() as tempdir:
            copy_from_export_path(
                export_path(task, ds), os.path.join(tempdir, task + "." + file_format)
            )

            if task == "blocks":
                insert_blocks(
                    os.path.join(tempdir, task + "." + file_format), engine, session
                )
            elif task == "transactions":
                insert_transactions(
                    os.path.join(tempdir, task + "." + file_format), engine, session
                )
            elif task == "receipts":
                insert_receipts(
                    os.path.join(tempdir, task + "." + file_format), engine, session
                )
            elif task == "logs":
                insert_logs(
                    os.path.join(tempdir, task + "." + file_format), engine, session
                )
            else:
                raise NotImplementedError(
                    "Insertion of {} is not yet supported.".format(task)
                )

    load_blocks_task = add_load_tasks("blocks", "csv")
    load_transactions_task = add_load_tasks("transactions", "csv")
    load_receipts_task = add_load_tasks("receipts", "csv")
    load_logs_task = add_load_tasks("logs", "json")

    return dag

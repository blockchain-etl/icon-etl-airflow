from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from iconetl_airflow.variables import read_var
import os
import logging
from tempfile import TemporaryDirectory
import csv

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
start_date = read_var(
    "nightly_reports_start_date",
    var_prefix="icon_",
    required=False,
    nightly_reports_start_date="2018-01-24",
)
cloud_provider = read_var(
    "cloud_provider", var_prefix="icon_", required=False, cloud_provider="gcp"
)

output_bucket = read_var("output_bucket", var_prefix="icon_", required=True)

if cloud_provider == "aws":
    from airflow.hooks.S3_hook import S3Hook

    cloud_storage_hook = S3Hook(aws_conn_id="aws_default")
if cloud_provider == "gcp":
    from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
    from iconetl_airflow.build_export_dag import upload_to_gcs

    cloud_storage_hook = GoogleCloudStorageHook(
        google_cloud_storage_conn_id="google_cloud_default"
    )

default_dag_args = {
    "depends_on_past": False,
    "start_date": start_date,
}

dag = DAG(
    "icon_generate_nightly_reports",
    schedule_interval="30 1 * * *",
    default_args=default_dag_args,
)

reports_folder = os.path.dirname(os.path.realpath(__file__)) + "/nightly_report_scripts"


def report_path(date):
    return "reports/report_date={report_date}/".format(
        report_date=date.strftime("%Y-%m-%d")
    )


def copy_to_report_path(file_path, report_path):
    logging.info("Calling copy_to_report_path({}, {})".format(file_path, report_path))
    filename = os.path.basename(file_path)

    if cloud_provider == "aws":
        cloud_storage_hook.load_file(
            filename=file_path,
            bucket_name=output_bucket,
            key=report_path + filename,
            replace=True,
            encrypt=False,
        )
    if cloud_provider == "gcp":
        upload_to_gcs(
            gcs_hook=cloud_storage_hook,
            bucket=output_bucket,
            object=report_path + filename,
            filename=file_path,
        )


def sql_task(execution_date, input_file, **kwargs):
    if os.path.exists(os.path.join(reports_folder, input_file)):
        pg_hook = PostgresHook("postgres")
        pg_cursor = pg_hook.get_cursor()

        try:
            sql_file = open(os.path.join(reports_folder, input_file))
            sql = sql_file.read()
        finally:
            sql_file.close()

        pg_cursor.execute(sql)
        result = pg_cursor.fetchall()

        with TemporaryDirectory() as tempdir:
            with open(
                os.path.join(tempdir, input_file + "-report.csv"), "w"
            ) as out_file:
                wr = csv.writer(out_file, quoting=csv.QUOTE_ALL)
                wr.writerows(result)

            copy_to_report_path(
                os.path.join(tempdir, input_file + "-report.csv"),
                report_path(execution_date),
            )

    else:
        return FileNotFoundError


def add_sql_task(input_file):
    load_operator = PythonOperator(
        task_id="nightly_report_{input_file}".format(input_file=input_file),
        dag=dag,
        python_callable=sql_task,
        provide_context=True,
        op_kwargs={"input_file": input_file},
    )
    return load_operator


for _, _, files in os.walk(reports_folder):
    for name in files:
        add_sql_task(name)

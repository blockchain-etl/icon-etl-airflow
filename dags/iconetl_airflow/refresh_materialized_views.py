from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime


def build_refresh_materialized_views_dag(
    dag_id,
    chain="icon",
    start_date=datetime(2018, 1, 24),
    db_maintenance_schedule_interval="59 0 * * *",
    **kwargs
):

    default_dag_args = {
        "depends_on_past": False,
        "start_date": start_date,
    }

    dag = DAG(
        dag_id,
        schedule_interval=db_maintenance_schedule_interval,
        default_args=default_dag_args,
    )

    def refresh_reduced_transactions():
        return PostgresOperator(
            task_id="refresh_reduced_transactions",
            postgres_conn_id="postgres",
            sql="REFRESH MATERIALIZED VIEW reduced_trans;",
            dag=dag,
        )

    def refresh_current_period():
        return PostgresOperator(
            task_id="refresh_current_period",
            postgres_conn_id="postgres",
            sql="REFRESH MATERIALIZED VIEW current_period;",
            dag=dag,
        )

    def refresh_previous_period():
        return PostgresOperator(
            task_id="refresh_previous_period",
            postgres_conn_id="postgres",
            sql="REFRESH MATERIALIZED VIEW previous_period;",
            dag=dag,
        )

    refresh_reduced_transactions_task = refresh_reduced_transactions()
    refresh_current_period_task = refresh_current_period()
    refresh_previous_period_task = refresh_previous_period()

    refresh_reduced_transactions_task >> refresh_current_period_task
    refresh_reduced_transactions_task >> refresh_previous_period_task

    return dag

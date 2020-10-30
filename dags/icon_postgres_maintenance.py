from iconetl_airflow.refresh_materialized_views import (
    build_refresh_materialized_views_dag,
)
from iconetl_airflow.variables import read_analytics_db_maintenance_vars
from iconetl_airflow.variables import read_var

database = read_var("database", var_prefix="icon_", required=False, database="bigquery")

if database == "postgres":
    DAG = build_refresh_materialized_views_dag(
        dag_id="icon_db_maintenance_dag",
        chain="icon",
        **read_analytics_db_maintenance_vars(
            var_prefix="icon_",
            db_maintenance_schedule_interval="59 12 * * *",
            maintenance_start_date="2018-01-24",
        )
    )

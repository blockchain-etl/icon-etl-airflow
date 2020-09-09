from __future__ import print_function

from iconetl_airflow.build_export_dag import build_export_dag
from iconetl_airflow.variables import read_export_dag_vars

# airflow DAG
DAG = build_export_dag(
    dag_id='icon_export_dag',
    **read_export_dag_vars(
        var_prefix='icon_',
        export_schedule_interval='0 13 * * *',
        export_start_date='2018-01-24',
        export_max_workers=10,
        export_batch_size=10,
        export_retries=5,
    )
)
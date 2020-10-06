from __future__ import print_function

import logging

from iconetl_airflow.build_load_dag import build_load_dag
from iconetl_airflow.build_load_dag_redshift import build_load_dag_redshift
from iconetl_airflow.build_load_dag_postgres import build_load_dag_postgres
from iconetl_airflow.variables import read_load_dag_vars
from iconetl_airflow.variables import read_load_dag_redshift_vars
from iconetl_airflow.variables import read_load_dag_postgres_vars
from iconetl_airflow.variables import read_var

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

# Default is gcp
cloud_provider = read_var('cloud_provider', var_prefix='icon_', required=False, cloud_provider='gcp')
database = read_var('database', var_prefix='icon_', required=False, database='bigquery')

if database == 'bigquery':
    # airflow DAG
    DAG = build_load_dag(
        dag_id='icon_load_dag',
        chain='icon',
        **read_load_dag_vars(
            var_prefix='icon_',
            schedule_interval='30 12 * * *',
            import_start_date='2018-01-24',
        )
    )
elif database == 'redshift':
    # airflow DAG
    DAG = build_load_dag_redshift(
        dag_id='icon_load_dag',
        chain='icon',
        **read_load_dag_redshift_vars(
            var_prefix='icon_',
            schedule_interval='30 12 * * *',
            import_start_date='2018-01-24',
        )
    )
elif database == 'postgres':
    DAG = build_load_dag_postgres(
        dag_id='icon_load_dag',
        chain='icon',
        **read_load_dag_postgres_vars(
            var_prefix='icon_',
            schedule_interval='30 12 * * *',
            import_start_date='2018-01-24',
        )
    )
else:
    raise ValueError('You must set a valid database Airflow variable (bigquery, redshift, postgres)')
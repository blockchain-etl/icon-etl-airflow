# icon-etl-airflow

To use with [terragrunt-icon-analytics:](https://github.com/insight-icon/terragrunt-icon-analytics)

- copy contents of ./dags folder into /etc/airflow/dags and ensure airflow is the owner of files
- install requirements as the airflow user
  - sudo su - airflow
  - pip3 install -r requirements.txt
- restart airflow-scheduler and airflow-webserver to load new packages
- set mandatory variables
  - icon_cloud_provider: aws
  - icon_database: postgres
  - icon_output_bucket: whatever-your-bucket-is-named
  - icon_provider_uris: https://ctz.solidwallet.io/api/v3
- configure postgres connection in connections tab
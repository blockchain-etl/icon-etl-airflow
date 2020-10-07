# icon-etl-airflow


### Contents
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->


- [Usage](#usage)
- [Variables](#variables)
  - [Project-wide](#project-wide)
  - [Export](#export)
  - [Load](#load)
- [Schemas](#schemas)
  - [Blocks](#blocks)
  - [Transactions](#transactions)
  - [Logs](#logs)
  - [Receipts](#receipts)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Usage

To use with [terragrunt-icon-analytics:](https://github.com/insight-icon/terragrunt-icon-analytics)

- copy contents of ./dags folder into /etc/airflow/dags and ensure airflow is the owner of files

```bash
sudo cp ./dags /etc/airflow/dags
sudo chown -R airflow:airflow /etc/airflow/dags
```

- install requirements as the airflow user
```bash
sudo su - airflow
pip3 install -r requirements.txt
exit
```

- restart airflow-scheduler and airflow-webserver to load new packages
```
sudo systemctl restart airflow-scheduler
sudo systemctl restart airflow-webserver
```

- set mandatory variables
  - icon_cloud_provider: aws
  - icon_database: postgres
  - icon_output_bucket: whatever-your-bucket-is-named
  - icon_provider_uris: https://ctz.solidwallet.io/api/v3
- configure postgres connection in connections tab

## Variables

### Project-wide

| Variable                      	| Suggested Default                 	| Description                                                        	|
|-------------------------------	|-----------------------------------	|--------------------------------------------------------------------	|
| icon_cloud_provider           	| aws                               	| The cloud provider you're using for Airflow: either 'aws' or 'gcp' 	|
| icon_database                 	| postgres                          	| The connection name for the database you're inserting data into    	|
| icon_output_bucket            	|                                   	| The name of the output bucket for block data                       	|
| icon_provider_uris            	| https://ctz.solidwallet.io/api/v3 	| The URI of the endpoint to use for block data                      	|
| notification_emails           	|                   	                | List of email addresses to notify about job status                   	|

### Export

| Variable                                   	|   Default  	| Description                                           	|
|--------------------------------------------	|:----------:	|-------------------------------------------------------	|
| icon_export_start_date                     	| 2018-01-24 	| The first date to start exporting                     	|
| icon_export_schedule_interval              	|  0 0 * * * 	| The scheduled time for when the export job should run 	|
| icon_export_max_active_runs                	|    None    	| The maximum number of active export runs              	|
| icon_export_max_workers                    	|     10     	| The maximum number of export workers per run          	|
| icon_export_batch_size                     	|     10     	| The export batch size                                 	|
| icon_export_retries                        	|      5     	| The number of retries per export run                  	|
| icon_export_blocks_and_transactions_toggle 	|    True    	| Enable exporting of blocks and transactions           	|
| icon_export_receipts_and_logs_toggle       	|    True    	| Enable exporting of receipts and logs                 	|

### Load

| Variable                       	| Default 	| Description                               	|
|--------------------------------	|:-------:	|-------------------------------------------	|
| aws_access_key_id              	|         	| Access key ID to use for AWS Redshift     	|
| aws_secret_access_key          	|         	| Secret access key to use for AWS Redshift 	|
| load_all_partitions            	|   true  	| Load all partitions in BigQuery           	|
| destination_dataset_project_id 	|         	| GCP Project ID for the BigQuery dataset   	|

## Schemas

Further information about the data used in the schemas is available in the [ICON JSON RPC documentation.](https://www.icondev.io/docs/icon-json-rpc-v3)

Primary keys for each table are **indicated in bold**.

Caution should be exercised when using the timestamp field directly from the API.
The time resolution of the epoch timestamp changes throughout.
For ease of use, it is suggested to perform a transformation on this column in all tables to use only the left 16 digits (corresponding to the epoch timestamp in seconds).

A suggested transformation query is:

```sql
select left(timestamp::text, 10)::int8 as timestamp_s
```

and can be used to create a transformed column in a new table or materialized view.
### Blocks

| Field            	| Type   	|
|------------------	|--------	|
| number           	| bigint 	|
| hash             	| string 	|
| parent_hash      	| string 	|
| merkle_root_hash 	| string 	|
| timestamp        	| bigint 	|
| version          	| string 	|
| peer_id          	| string 	|
| signature        	| string 	|
| next_leader      	| string 	|

### Transactions

| Field             	| Type           	|
|-------------------	|----------------	|
| version           	| string         	|
| from_address      	| string         	|
| to_address        	| string         	|
| value             	| numeric(38,0)  	|
| step_limit        	| numeric(38,0)  	|
| timestamp         	| bigint         	|
| nid               	| int            	|
| nonce             	| numeric(100,0) 	|
| **hash**          	| **string**     	|
| transaction_index 	| bigint         	|
| block_hash        	| string         	|
| block_number      	| bigint         	|
| fee               	| numeric(38,0)  	|
| signature         	| string         	|
| data_type         	| string         	|
| data              	| string         	|

### Logs

| Field                 	| Type       	|
|-----------------------	|------------	|
| **log_index**         	| **int**    	|
| **transaction_hash**  	| **string** 	|
| **transaction_index** 	| **int**    	|
| block_hash            	| string     	|
| block_number          	| int        	|
| address               	| string     	|
| data                  	| string     	|
| indexed               	| string     	|

### Receipts

| Field                 	| Type          	|
|-----------------------	|---------------	|
| **transaction_hash**  	| **string**    	|
| **transaction_index** 	| **int**       	|
| block_hash            	| string        	|
| block_number          	| int           	|
| cumulative_step_used  	| numeric(38,0) 	|
| step_used             	| numeric(38,0) 	|
| step_price            	| numeric(38,0) 	|
| score_address         	| string        	|
| status                	| string        	|

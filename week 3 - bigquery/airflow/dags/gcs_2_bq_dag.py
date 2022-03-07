import os
import logging
from datetime import datetime



from airflow import DAG
from airflow.utils.dates import days_ago



from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator,BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

path_to_local_home = os.environ.get('AIRFLOW_HOME','/opt/airflow')

BUCKET = os.getenv('GCP_GCS_BUCKET')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET','trips_data_all')
PROJECT_ID = os.getenv('GCP_PROJECT_ID')

default_args = {
    "owner":"airflow",
    "start_date":days_ago(1),
    "depends_on_past": False,
    "retries":1
}

# gcs -> bq_ext_table -> part
with DAG(
    dag_id = "yellow_taxi_gcs_dag",
    schedule_interval = "@once",
    default_args = default_args,
    max_active_runs=1,
    tags = ['dtc-de'],
) as dag:

    for task in ["green"]:
        gcs_2_gcs_task = GCSToGCSOperator(
            task_id=f'{task}_gcs_2_gcs_task',
            source_bucket=BUCKET,
            source_objects=[f'raw/{task}_tripdata/2019', f'raw/{task}_tripdata/2020'],
            destination_bucket=BUCKET,
            destination_object = f"{task}",
            move_object=True,
            
        )

        gcs_2_bq_ext_task = BigQueryCreateExternalTableOperator(
            task_id = f"{task}_gcs_2_bq_ext_task",
            table_resource = {
                "tableReference" : {
                    "projectId":PROJECT_ID,
                    "datasetId":BIGQUERY_DATASET,
                    "tableId":f"external_{task}_tripdata_airflow",
                },
                "externalDataConfiguration":{
                    "sourceFormat":"PARQUET",
                    "sourceUris":[f"gs://{BUCKET}/{task}/*"],
                }
            }
        )
        PARTITION_FIELD = "tpep_pickup_datetime" if task == 'green' else 'pickup_datetime'
        CREATE_PARTITION_TABLE_QUERY = f"CREATE OR REPLACE TABLE de-first-340309.{BIGQUERY_DATASET}.{task}_tripdata_partitioned_airflow PARTITION BY  \
                            DATE({PARTITION_FIELD}) AS \
                            select * from {BIGQUERY_DATASET}.external_{task}_tripdata_airflow;"
        bq_ext_2_part_task = BigQueryInsertJobOperator(
        task_id=f"{task}_bq_ext_2_part_task",
        configuration={
            "query": {
                "query": CREATE_PARTITION_TABLE_QUERY,
                "useLegacySql": False,
            }
        },
        
        )

        gcs_2_gcs_task >> gcs_2_bq_ext_task >> bq_ext_2_part_task
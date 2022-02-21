import os
import logging
from datetime import datetime



from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# from google.cloud import storage
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
# import pyarrow.csv as pv
# import pyarrow.parquet as pq
from scripts import format_to_parquet, upload_to_gcs

PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BUCKET = os.getenv('GCP_GCS_BUCKET')

dataset_file = 'yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv'
dataset_url = f'https://s3.amazonaws.com/nyc-tlc/trip+data/{dataset_file}'
path_to_local_home = os.environ.get('AIRFLOW_HOME','/opt/airflow')
parquet_file = "output_{{execution_date.strftime(\'%Y-%m\')}}.parquet"

BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET','trips_data_all')
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME','/opt/airflow')

URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data/"
URL_TEMPALTE = URL_PREFIX + "yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv"
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{execution_date.strftime(\'%Y-%m\')}}.csv'
TABLE_NAME_TEMPALTE = 'yellow_taxi_{{execution_date.strftime(\'%Y_%m\')}}'
GCP_PATH_TEMPLATE = "raw/yellow_tripdata/{{execution_date.strftime(\'%Y\')}}/"+parquet_file



default_args = {
    "owner":"airflow",
    "depends_on_past": False,
    "retries":1
}

# DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id = "yellow_taxi_data_dag",
    schedule_interval = "@monthly",
    default_args = default_args,
    start_date =datetime(2019,1,1),
    end_date =datetime(2020,12,31),
    catchup =  True,
    max_active_runs = 3,
    tags = ['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id = "download_dataset_task",
        bash_command = f"curl -sSLf {URL_TEMPALTE} > {OUTPUT_FILE_TEMPLATE}"
    )

    format_to_parquet_task = PythonOperator(
        task_id ="format_to_parquet_task",
        python_callable = format_to_parquet,
        op_kwargs={
            "src_file":OUTPUT_FILE_TEMPLATE
        },
    )
    # TODO: research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id = "local_to_gcs_task",
        python_callable = upload_to_gcs,
        op_kwargs = {
            "bucket":BUCKET,
            "object_name":GCP_PATH_TEMPLATE,
            "local_file":f"{path_to_local_home}/{parquet_file}",
        },
    )
    remove_dataset_task = BashOperator(
        task_id = "remove_dataset_task",
        bash_command = f"rm {path_to_local_home}/{parquet_file} {OUTPUT_FILE_TEMPLATE}"
    )

    # bigquery_external_dataset_task = BigQueryCreateExternalTableOperator(
    #     task_id = "bigquery_external_dataset_task",
    #     table_resource = {
    #         "tableReference" : {
    #             "projectId":PROJECT_ID,
    #             "datasetId":BIGQUERY_DATASET,
    #             "tableId":"external_table",
    #         },
    #         "externalDataConfiguration":{
    #             "sourceFormat":"PARQUET",
    #             "sourceUris":[f"gs://{BUCKET}/raw/{parquet_file}"],
    #         }
    #     }
    # )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> remove_dataset_task


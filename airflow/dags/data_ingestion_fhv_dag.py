import os
import logging
from datetime import datetime



from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
#from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

from scripts import format_to_parquet, upload_to_gcs

PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BUCKET = os.getenv('GCP_GCS_BUCKET')

fhv_parquet_file = "output_fhv_{{execution_date.strftime(\'%Y-%m\')}}.parquet"
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET','trips_data_all')
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME','/opt/airflow')
path_to_local_home = os.environ.get('AIRFLOW_HOME','/opt/airflow')
URL_PREFIX = "https://nyc-tlc.s3.amazonaws.com/trip+data/"

fhv_datafile = 'fhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv'
FHV_URL_TEMPALTE = URL_PREFIX + "fhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv"
FHV_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_fhv_{{execution_date.strftime(\'%Y-%m\')}}.csv'
GCP_PATH_TEMPLATE = "raw/fhv_tripdata/{{execution_date.strftime(\'%Y\')}}/"+fhv_parquet_file

default_args = {
    "owner":"airflow",
    "depends_on_past": False,
    "retries":1
}

with DAG(
    dag_id = "fhv_taxi_dag",
    schedule_interval = "@monthly",
    default_args = default_args,
    catchup =  True,
    start_date =datetime(2019,1,1),
    end_date =datetime(2019,12,31),
    max_active_runs = 3,
    tags = ['dtc-de'],
) as dag:
    download_dataset_task = BashOperator(
        task_id = "download_fhv_task",
        bash_command = f"curl -sSLf {FHV_URL_TEMPALTE} > {FHV_OUTPUT_FILE_TEMPLATE}"
    )

    format_to_parquet_task = PythonOperator(
        task_id ="format_to_parquet_task",
        python_callable = format_to_parquet,
        op_kwargs={
            "src_file":f"{FHV_OUTPUT_FILE_TEMPLATE}"
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id = "local_to_gcs_task",
        python_callable = upload_to_gcs,
        op_kwargs = {
            "bucket":BUCKET,
            "object_name":GCP_PATH_TEMPLATE,
            "local_file":f"{path_to_local_home}/{fhv_parquet_file}",
        },
    )
    remove_dataset_task = BashOperator(
        task_id = "remove_dataset_task",
        bash_command = f"rm {path_to_local_home}/{fhv_parquet_file} {FHV_OUTPUT_FILE_TEMPLATE}"
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> remove_dataset_task
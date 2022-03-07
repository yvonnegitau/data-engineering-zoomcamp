import os
import logging
from datetime import datetime



from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


from scripts import format_to_parquet, upload_to_gcs

parquet_file = "output_taxi+_zone_lookup.csv".replace(".csv",'.parquet')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET','trips_data_all')
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME','/opt/airflow')
path_to_local_home = os.environ.get('AIRFLOW_HOME','/opt/airflow')
URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data/"

fhv_datafile = "taxi+_zone_lookup.csv"
FHV_URL_TEMPALTE = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
FHV_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_taxi+_zone_lookup.csv'

PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BUCKET = os.getenv('GCP_GCS_BUCKET')


default_args = {
    "owner":"airflow",
    "depends_on_past": False,
    "retries":1
}

with DAG(
    dag_id = "data_ingestion_zones_dag",
    schedule_interval = "@once",
    default_args = default_args,
    catchup =  False,
    start_date =datetime.now(),
    max_active_runs = 1,
    tags = ['dtc-de'],
) as dag:
    download_dataset_task = BashOperator(
        task_id = "download_zone_task",
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
            "object_name":f"raw/{parquet_file}",
            "local_file":f"{path_to_local_home}/{parquet_file}",
        },
    )
    remove_dataset_task = BashOperator(
        task_id = "remove_dataset_task",
        bash_command = f"rm {path_to_local_home}/{parquet_file} {FHV_OUTPUT_FILE_TEMPLATE}"
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> remove_dataset_task
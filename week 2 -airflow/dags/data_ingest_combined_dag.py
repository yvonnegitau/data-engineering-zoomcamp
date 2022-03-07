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

path_to_local_home = os.environ.get('AIRFLOW_HOME','/opt/airflow')
URL_PREFIX = "https://nyc-tlc.s3.amazonaws.com/trip+data/"


FHV_URL_TEMPALTE = URL_PREFIX + "fhv_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv"
CSV_OUTPUT_FILE_TEMPLATE = path_to_local_home + '/output_fhv_{{execution_date.strftime(\'%Y-%m\')}}.csv'
PARQUET_OUTPUT_FILE_TEMPLATE = path_to_local_home + '/output_fhv_{{execution_date.strftime(\'%Y-%m\')}}.parquet'
GCP_PATH_TEMPLATE = "raw/fhv_tripdata/{{execution_date.strftime(\'%Y\')}}/output_fhv_{{execution_date.strftime(\'%Y-%m\')}}.parquet"
BUCKET = os.getenv('GCP_GCS_BUCKET')


def download_parquetize_upload_dag(
    dag,
    url_template,
    local_csv_path_template,
    local_parquet_path_template,
    gcs_path_tempate
    ):
    with dag:
        download_dataset_task = BashOperator(
        task_id = "download_dataset_task",
        bash_command = f"curl -sSLf {url_template} > {local_csv_path_template}"
        )

        format_to_parquet_task = PythonOperator(
            task_id ="format_to_parquet_task",
            python_callable = format_to_parquet,
            op_kwargs={
                "src_file":f"{local_csv_path_template}"
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id = "local_to_gcs_task",
            python_callable = upload_to_gcs,
            op_kwargs = {
                "bucket":BUCKET,
                "object_name":gcs_path_tempate,
                "local_file":local_parquet_path_template,
            },
        )
        remove_dataset_task = BashOperator(
            task_id = "remove_dataset_task",
            bash_command = f"rm {local_parquet_path_template} {local_csv_path_template}"
        )

        download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> remove_dataset_task


default_args = {
    "owner":"airflow",
    "depends_on_past": False,
    "retries":1
}

fhv_taxi = DAG(
    dag_id = "fhv_taxi_dag",
    schedule_interval = "@monthly",
    default_args = default_args,
    catchup =  True,
    start_date =datetime(2019,1,1),
    end_date =datetime(2019,12,31),
    max_active_runs = 3,
    tags = ['dtc-de'],
)

download_parquetize_upload_dag(dag = fhv_taxi,
url_template =FHV_URL_TEMPALTE ,
local_csv_path_template = CSV_OUTPUT_FILE_TEMPLATE,
local_parquet_path_template = PARQUET_OUTPUT_FILE_TEMPLATE,
gcs_path_tempate = GCP_PATH_TEMPLATE
)


URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data/"


YELLOW_URL_TEMPALTE = URL_PREFIX + "yellow_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv"
YELLOW_CSV_OUTPUT_FILE_TEMPLATE = path_to_local_home + '/output_{{execution_date.strftime(\'%Y-%m\')}}.csv'
YELLOW_PARQUET_OUTPUT_FILE_TEMPLATE = path_to_local_home + '/output_{{execution_date.strftime(\'%Y-%m\')}}.parquet'
YELLOW_GCP_PATH_TEMPLATE = "raw/yellow_tripdata/{{execution_date.strftime(\'%Y\')}}/output_{{execution_date.strftime(\'%Y-%m\')}}.parquet"

yellow_taxi = DAG(
    dag_id = "yellow_taxi_data_dag",
    schedule_interval = "@monthly",
    default_args = default_args,
    start_date =datetime(2019,1,1),
    end_date =datetime(2020,12,31),
    catchup =  True,
    max_active_runs = 3,
    tags = ['dtc-de'],
)

download_parquetize_upload_dag(dag = yellow_taxi,
url_template =YELLOW_URL_TEMPALTE ,
local_csv_path_template = YELLOW_CSV_OUTPUT_FILE_TEMPLATE,
local_parquet_path_template = YELLOW_PARQUET_OUTPUT_FILE_TEMPLATE,
gcs_path_tempate = YELLOW_GCP_PATH_TEMPLATE
)

URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data/"


GREEN_URL_TEMPALTE = URL_PREFIX + "green_tripdata_{{execution_date.strftime(\'%Y-%m\')}}.csv"
GREEN_CSV_OUTPUT_FILE_TEMPLATE = path_to_local_home + '/output_{{execution_date.strftime(\'%Y-%m\')}}.csv'
GREEN_PARQUET_OUTPUT_FILE_TEMPLATE = path_to_local_home + '/output_{{execution_date.strftime(\'%Y-%m\')}}.parquet'
GREEN_GCP_PATH_TEMPLATE = "raw/green_tripdata/{{execution_date.strftime(\'%Y\')}}/output_{{execution_date.strftime(\'%Y-%m\')}}.parquet"

green_taxi = DAG(
    dag_id = "green_taxi_data_dag",
    schedule_interval = "@monthly",
    default_args = default_args,
    start_date =datetime(2019,1,1),
    end_date =datetime(2020,12,31),
    catchup =  True,
    max_active_runs = 3,
    tags = ['dtc-de'],
)

download_parquetize_upload_dag(dag = green_taxi,
url_template =GREEN_URL_TEMPALTE ,
local_csv_path_template = GREEN_CSV_OUTPUT_FILE_TEMPLATE,
local_parquet_path_template = GREEN_PARQUET_OUTPUT_FILE_TEMPLATE,
gcs_path_tempate = GREEN_GCP_PATH_TEMPLATE
)

URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data/"


ZONES_URL_TEMPALTE = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
ZONES_CSV_OUTPUT_FILE_TEMPLATE = path_to_local_home + '/output_taxi+_zone_lookup.csv'
ZONES_PARQUET_OUTPUT_FILE_TEMPLATE = path_to_local_home + '/output_taxi+_zone_lookup.parquet'
ZONES_GCP_PATH_TEMPLATE = "raw/zones/{{execution_date.strftime(\'%Y\')}}/output_taxi+_zone_lookup.parquet"

zones = DAG(
    dag_id = "data_ingestion_zones_dag",
    schedule_interval = "@once",
    default_args = default_args,
    catchup =  False,
    start_date =datetime.now(),
    max_active_runs = 1,
    tags = ['dtc-de'],
)

download_parquetize_upload_dag(dag = zones,
url_template =ZONES_URL_TEMPALTE ,
local_csv_path_template = ZONES_CSV_OUTPUT_FILE_TEMPLATE,
local_parquet_path_template = ZONES_PARQUET_OUTPUT_FILE_TEMPLATE,
gcs_path_tempate = ZONES_GCP_PATH_TEMPLATE
)
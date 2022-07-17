"""
This file configures the Apache Airflow DAG to (re)ingest Inaturalist data.

It is separate from .provider_workflows, to include the initial steps of loading data
from S3 to Postgres, but then it returns to the ordinary `ProviderDataIngester` flow,
with a modified `get_response_json` that retrieves paginated data from Postgres rather
than calling an API.
"""
import logging
import os

# airflow DAG (necessary for Airflow to find this file)
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator

# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.task_group import TaskGroup
from common.constants import DAG_DEFAULT_ARGS
from providers.provider_api_scripts.inaturalist import inaturalistDataIngester


logging.basicConfig(
    format="%(asctime)s: [%(levelname)s - DAG Loader] %(message)s", level=logging.DEBUG
)
logger = logging.getLogger(__name__)

AWS_CONN_ID = os.getenv("AWS_CONN_ID", "test_conn_id")

DAG_ID = "inaturalist_workflow"

SOURCE_FILE_NAMES = ["photos", "observations", "taxa", "observers"]

INAT = inaturalistDataIngester()

# def check_if_files_updated(
#     last_success: pendulum.DateTime | None,
#     s3_keys: list,
#     aws_conn_id=AWS_CONN_ID
#     ):
#     # if it was never run, assume the data is new
#     if last_success is None:
#         return
#     s3 = S3Hook(aws_conn_id=aws_conn_id)
#     for key in s3_keys:
#         last_modified = s3.head_object(key)['Last-Modified']
#         # if any file has been updated, let's pull them all
#         if last_success < last_modified:
#             return
#     # If no files have been updated, skip the DAG
#     raise AirflowSkipException("Nothing new to ingest")
 
with DAG(
    DAG_ID,
    default_args=DAG_DEFAULT_ARGS,
    description="Import raw inaturalist data from S3 to Postgres and ingest normally",
    schedule_interval="@monthly",
    catchup=False,
    tags=["inaturalist", "provider: image"],
) as dag:

    with TaskGroup(group_id="check_sources_exist") as check_sources_exist:
        for source_name in SOURCE_FILE_NAMES:
            S3KeySensor(
                task_id=source_name + "_exists",
                bucket_key=f"s3://inaturalist-open-data/{source_name}.csv.gz",
                aws_conn_id=AWS_CONN_ID,
                poke_interval=15,
                mode="reschedule",
                timeout=60,
            )

    # check_if_updated = PythonOperator(
    #     task_id="check_if_updated",
    #     python_callable=check_if_files_updated,
    #     op_kwargs={
    #         # With the templated values ({{ x }}) airflow will fill it in for us
    #         "last_success": "{{ prev_start_date_success }}",
    #         "s3_keys": [f"s3://inaturalist/{file}.csv.gz"
    # for file in SOURCE_FILE_NAMES]
    #     }
    # )

    create_schema = PythonOperator(
        task_id="create_schema",
        python_callable=INAT.sql_loader,
        op_kwargs={"file_name": "00_create_schema.sql"},
    )

    with TaskGroup(group_id="load_source_files") as load_source_files:
        operator_list = [
            PythonOperator(
                task_id=f"load_{source_name}",
                python_callable=INAT.sql_loader,
                op_kwargs={"file_name": str(idx + 1).zfill(2) + f"_{source_name}.sql"},
            )
            for idx, source_name in enumerate(SOURCE_FILE_NAMES)
        ]
        chain(*operator_list)

    ingest_data = PythonOperator(
        task_id="ingest_data", python_callable=INAT.ingest_records
    )

    (
        check_sources_exist
        # >> check_if_updated
        >> create_schema
        >> load_source_files
        >> ingest_data
    )

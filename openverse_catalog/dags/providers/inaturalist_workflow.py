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
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.utils.task_group import TaskGroup
from common.constants import DAG_DEFAULT_ARGS
from providers.provider_api_scripts import inaturalist


logging.basicConfig(
    format="%(asctime)s: [%(levelname)s - DAG Loader] %(message)s", level=logging.DEBUG
)
logger = logging.getLogger(__name__)

AWS_CONN_ID = os.getenv("AWS_CONN_ID", "test_conn_id")

DAG_ID = "inaturalist_workflow"

SOURCE_FILE_NAMES = ["observations", "observers", "photos", "taxa"]

with DAG(
    DAG_ID,
    default_args=DAG_DEFAULT_ARGS,
    description="Import raw inaturalist data from S3 to Postgres and ingest normally",
    schedule_interval="@monthly",
    catchup=False,
    tags=["inaturalist", "provider: image"],
) as dag:

    # Eventually, we'll probably want a function here to confirm that the files have
    # been updated since the last time we loaded them. But for now, just trying to see
    # if waiting to load the data might help with intermittent "file doesn't exist" in
    # the test environment.
    # Also, in theory s3KeySensor can handle a list of keys
    # (https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/sensors/s3/index.html#airflow.providers.amazon.aws.sensors.s3.S3KeySensor)
    # but I'm getting weird type errors when I try to use it, so doing this task
    # group instead for now.
    with TaskGroup(group_id="check_sources_exist") as check_sources_exist:
        upstream_task = None
        for source_name in SOURCE_FILE_NAMES:
            globals()[source_name + "_exists"] = S3KeySensor(
                task_id=source_name + "_exists",
                bucket_key="s3://inaturalist-open-data/" + source_name + ".csv.gz",
                aws_conn_id=AWS_CONN_ID,
                poke_interval=5,
            )
            if upstream_task is not None:
                globals()[source_name + "_exists"].set_upstream(upstream_task)
            upstream_task = globals()[source_name + "_exists"]

    create_schema = PythonOperator(
        task_id="create_schema",
        python_callable=inaturalist.create_schema,
    )

    # There is a much more elegant and pythonic way to generate the callables
    # themselves too, but first let's get the basics running
    load_callables = [
        inaturalist.load_photos,
        inaturalist.load_observations,
        inaturalist.load_observers,
        inaturalist.load_taxa,
    ]
    with TaskGroup(group_id="load_source_files") as load_source_files:
        upstream_task = None
        for (source_name, load_callable) in zip(SOURCE_FILE_NAMES, load_callables):
            globals()["load_" + source_name] = PythonOperator(
                task_id="load_" + source_name,
                python_callable=load_callable,
            )
        if upstream_task is not None:
            globals()["load_" + source_name].set_upstream(upstream_task)
        upstream_task = globals()["load_" + source_name]

    ingest_data = PythonOperator(
        task_id="ingest_data", python_callable=inaturalist.ingest_inaturalist_data
    )

    (check_sources_exist >> create_schema >> load_source_files >> ingest_data)

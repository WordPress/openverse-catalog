"""
This file configures the Apache Airflow DAG to (re)ingest Inaturalist data.

It is separate from .provider_workflows, to include the initial steps of loading data
from S3 to Postgres, but then it returns to the ordinary `ProviderDataIngester` flow,
with a modified `get_response_json` that retrieves paginated data from Postgres rather
than calling an API.
"""
import logging

# airflow DAG (necessary for Airflow to find this file)
from airflow import DAG
from airflow.operators.python import PythonOperator
from common.constants import DAG_DEFAULT_ARGS
from providers.provider_api_scripts import inaturalist


logging.basicConfig(
    format="%(asctime)s: [%(levelname)s - DAG Loader] %(message)s", level=logging.DEBUG
)
logger = logging.getLogger(__name__)

DAG_ID = "inaturalist_workflow"

with DAG(
    DAG_ID,
    default_args=DAG_DEFAULT_ARGS,
    description="Import raw inaturalist data from S3 to Postgres and ingest normally",
    schedule_interval="@monthly",
    catchup=False,
    tags=["inaturalist", "provider: image"],
) as dag:

    create_schema = PythonOperator(
        task_id="create_schema",
        python_callable=inaturalist.create_schema,
    )

    load_photos = PythonOperator(
        task_id="load_photos",
        python_callable=inaturalist.load_photos,
    )

    load_observations = PythonOperator(
        task_id="load_observations",
        python_callable=inaturalist.load_observations,
    )

    load_taxa = PythonOperator(
        task_id="load_taxa",
        python_callable=inaturalist.load_taxa,
    )

    load_observers = PythonOperator(
        task_id="load_observers",
        python_callable=inaturalist.load_observers,
    )

    ingest_data = PythonOperator(
        task_id="ingest_data", python_callable=inaturalist.ingest_inaturalist_data
    )

    # single thread just for testing, Airflow was running two things at once, and
    # seemed to be stuck in a never ending loop of retrying things that had already
    # finished.
    (
        create_schema
        >> load_photos
        >> load_observations
        >> load_observers
        >> load_taxa
        >> ingest_data
    )

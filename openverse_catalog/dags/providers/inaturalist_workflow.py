"""
This file configures the Apache Airflow DAG to (re)ingest Inaturalist data.
"""
import logging

# airflow DAG (necessary for Airflow to find this file)
from datetime import datetime

from common.provider_dag_factory import create_provider_api_workflow
from providers.provider_api_scripts import inaturalist


logging.basicConfig(
    format="%(asctime)s: [%(levelname)s - DAG Loader] %(message)s", level=logging.DEBUG
)
logger = logging.getLogger(__name__)

DAG_ID = "inaturalist_workflow"

globals()[DAG_ID] = create_provider_api_workflow(
    DAG_ID,
    inaturalist.main,
    start_date=datetime(1970, 1, 1),
    max_active_tasks=1,
    schedule_string="@monthly",
    dated=False,
)

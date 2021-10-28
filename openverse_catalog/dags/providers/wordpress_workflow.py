"""
This file configures the Apache Airflow DAG to (re)ingest WordPress data.
"""
import logging
from datetime import datetime

from providers.provider_api_scripts import wordpress
from util.dag_factory import create_provider_api_workflow


logging.basicConfig(
    format="%(asctime)s: [%(levelname)s - DAG Loader] %(message)s", level=logging.DEBUG
)
logger = logging.getLogger(__name__)

DAG_ID = "wordpress_workflow"

globals()[DAG_ID] = create_provider_api_workflow(
    DAG_ID,
    wordpress.main,
    start_date=datetime(1970, 1, 1),
    concurrency=1,
    schedule_string="@monthly",
    dated=False,
)

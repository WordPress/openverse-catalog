"""
This file configures the Apache Airflow DAG to update the database table to
reflect appropriate Smithsonian sub provider names in the source field
"""

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from util.loader import sql


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)

DAG_ID = "smithsonian_sub_provider_update_workflow"
DB_CONN_ID = os.getenv("OPENLEDGER_CONN_ID", "postgres_openledger_testing")
CONCURRENCY = 5

DAG_DEFAULT_ARGS = {
    "owner": "data-eng-admin",
    "depends_on_past": False,
    "start_date": datetime(2020, 1, 15),
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
    "schedule_interval": None,
}


def create_dag(
    dag_id=DAG_ID,
    args=DAG_DEFAULT_ARGS,
    concurrency=CONCURRENCY,
    max_active_runs=CONCURRENCY,
    postgres_conn_id=DB_CONN_ID,
):
    dag = DAG(
        dag_id=dag_id,
        default_args=args,
        concurrency=concurrency,
        max_active_runs=max_active_runs,
        catchup=False,
        schedule_interval=None,
    )

    with dag:
        PythonOperator(
            task_id="update_smithsonian_sub_providers",
            python_callable=sql.update_smithsonian_sub_providers,
            op_args=[postgres_conn_id],
        )

    return dag


globals()[DAG_ID] = create_dag()

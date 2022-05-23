"""
A maintenance one-off workflow that adds meta_data.license_url to all images.
"""
import os
from datetime import datetime, timedelta

import jinja2
from airflow.models import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from common import add_license_url, slack


DAG_ID = "add_license_url"
DB_CONN_ID = os.getenv("OPENLEDGER_CONN_ID", "postgres_openledger_testing")

# should we send someone an email when this DAG fails?
ALERT_EMAIL_ADDRESSES = ""
DAG_DEFAULT_ARGS = {
    "owner": "data-eng-admin",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 15),
    "template_undefined": jinja2.Undefined,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": slack.on_failure_callback,
}


def create_dag(dag_id=DAG_ID):
    dag = DAG(
        dag_id=dag_id,
        default_args=DAG_DEFAULT_ARGS,
        # If this was True, airflow would run this DAG in the beginning
        # for each day from the start day to now
        catchup=False,
        # Use the docstring at the top of the file as md docs in the UI
        doc_md=__doc__,
        tags=["data_normalization"],
    )

    with dag:
        make_sample_data = PythonOperator(
            task_id="make_sample_data",
            python_callable=add_license_url.make_sample_data,
            op_kwargs={"postgres_conn_id": DB_CONN_ID},
        )
        get_statistics = BranchPythonOperator(
            task_id="get_stats",
            python_callable=add_license_url.get_statistics,
            op_kwargs={"postgres_conn_id": DB_CONN_ID},
        )

        update_license_url = PythonOperator(
            task_id="update_license_url",
            python_callable=add_license_url.update_license_url,
            trigger_rule="all_done",
            op_kwargs={"postgres_conn_id": DB_CONN_ID},
        )
        final_report = PythonOperator(
            task_id="final_report",
            python_callable=add_license_url.final_report,
            op_kwargs={"postgres_conn_id": DB_CONN_ID},
        )

        get_statistics >> [make_sample_data, update_license_url]
        make_sample_data >> update_license_url
        update_license_url >> final_report

    return dag


globals()[DAG_ID] = create_dag()

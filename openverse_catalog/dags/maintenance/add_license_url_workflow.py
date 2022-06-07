"""
A maintenance one-off workflow that adds meta_data.license_url to all images.
"""
import os
from datetime import datetime, timedelta

import jinja2
from airflow.models import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from common import add_license_url, slack
from common.constants import XCOM_PULL_TEMPLATE


DAG_ID = "add_license_url"
DB_CONN_ID = os.getenv("OPENLEDGER_CONN_ID", "postgres_openledger_testing")

ALERT_EMAIL_ADDRESSES = ""
DAG_DEFAULT_ARGS = {
    "owner": "data-eng-admin",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 15),
    "template_undefined": jinja2.Undefined,
    "email_on_retry": False,
    "retries": 0,
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
        get_statistics = BranchPythonOperator(
            task_id="get_stats",
            python_callable=add_license_url.get_statistics,
            op_kwargs={"postgres_conn_id": DB_CONN_ID},
        )
        make_sample_data = PythonOperator(
            task_id="make_sample_data",
            python_callable=add_license_url.make_sample_data,
            op_kwargs={"postgres_conn_id": DB_CONN_ID},
        )
        add_blank_metadata = PythonOperator(
            task_id="add_blank_metadata",
            python_callable=add_license_url.add_blank_metadata,
            trigger_rule=TriggerRule.ALL_DONE,
            op_kwargs={"postgres_conn_id": DB_CONN_ID},
        )
        update_license_url = PythonOperator(
            task_id="update_license_url",
            python_callable=add_license_url.update_license_url,
            op_kwargs={"postgres_conn_id": DB_CONN_ID},
        )
        remove_license_url_from_meta_data = PythonOperator(
            task_id="remove_license_url_from_meta_data",
            python_callable=add_license_url.remove_license_url_from_meta_data,
            op_kwargs={"postgres_conn_id": DB_CONN_ID},
        )
        move_columns_to_metadata = PythonOperator(
            task_id="move_columns_to_metadata",
            python_callable=add_license_url.move_columns_to_metadata,
            op_kwargs={"postgres_conn_id": DB_CONN_ID},
        )
        final_report = PythonOperator(
            task_id="final_report",
            python_callable=add_license_url.final_report,
            op_kwargs={
                "postgres_conn_id": DB_CONN_ID,
                "item_count": XCOM_PULL_TEMPLATE.format(
                    update_license_url.task_id, "return_value"
                ),
            },
        )

        get_statistics >> [make_sample_data, add_blank_metadata]
        make_sample_data >> add_blank_metadata

        (
            add_blank_metadata
            >> update_license_url
            >> remove_license_url_from_meta_data
            >> move_columns_to_metadata
            >> final_report
        )

    return dag


globals()[DAG_ID] = create_dag()

"""
# Add license URL

Add `license_url` to all rows that have `NULL` in their `meta_data` fields.
The `license_url` is constructed from the `license` and `license_version` fields.

This is a maintenance DAG that should be run once.
"""
import logging
from datetime import datetime
from textwrap import dedent
from typing import Literal

import jinja2
from airflow.models import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from common import slack
from common.constants import POSTGRES_CONN_ID, XCOM_PULL_TEMPLATE
from common.licenses.constants import get_reverse_license_path_map
from common.loader.sql import RETURN_ROW_COUNT
from common.slack import send_message
from psycopg2._json import Json


DAG_ID = "add_license_url"
UPDATE_LICENSE_URL = "update_license_url"
FINAL_REPORT = "final_report"

ALERT_EMAIL_ADDRESSES = ""

logger = logging.getLogger(__name__)

base_url = "https://creativecommons.org/"


def get_statistics(
    postgres_conn_id: str,
) -> Literal[UPDATE_LICENSE_URL, FINAL_REPORT]:
    logger.info("Getting image records without license_url in meta_data.")
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    null_meta_data_count = postgres.get_first(
        dedent(
            """
        SELECT COUNT(*) from image
        WHERE meta_data IS NULL;
        """
        )
    )[0]

    logger.info(f"There are {null_meta_data_count} records with NULL in meta_data.")
    if null_meta_data_count > 0:
        return UPDATE_LICENSE_URL
    else:
        return FINAL_REPORT


def update_license_url(postgres_conn_id: str) -> dict[str, int]:
    """Add license_url to meta_data batching all records with the same license.
    :param postgres_conn_id: Postgres connection id
    """

    logger.info("Getting image records without license_url.")
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    license_map = get_reverse_license_path_map()

    total_count = 0
    total_counts = {}
    for license_items, path in license_map.items():
        license_name, license_version = license_items
        logger.info(f"Processing {license_name} {license_version}, {license_items}.")
        license_url = f"{base_url}{path}/"

        select_query = dedent(
            f"""
            SELECT identifier FROM image
            WHERE (
            meta_data is NULL AND license = '{license_name}'
            AND license_version = '{license_version}')
            """
        )
        result = postgres.get_records(select_query)

        if not result:
            logger.info(f"No records to update with {license_url}.")
            continue
        logger.info(f"{len(result)} records to update with {license_url}.")
        license_url_col = {"license_url": license_url}
        update_license_url_query = dedent(
            f"""
            UPDATE image
            SET meta_data = {Json(license_url_col)}
            WHERE identifier IN ({','.join([f"'{r[0]}'" for r in result])});
            """
        )

        updated_count = postgres.run(
            update_license_url_query, autocommit=True, handler=RETURN_ROW_COUNT
        )
        logger.info(f"{updated_count} records updated with {license_url}.")
        total_counts[license_url] = updated_count
        total_count += updated_count

    logger.info(f"{total_count} image records with missing license_url updated.")
    logger.info(f"Total updated counts: {total_counts}")
    return total_counts


def final_report(postgres_conn_id: str, item_count):
    logger.info(
        "Added license_url to all items. Checking for any records "
        "that still have NULL metadata."
    )
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    null_meta_data_records = postgres.get_first(
        dedent("SELECT COUNT(*) from image WHERE meta_data IS NULL;")
    )[0]
    logger.info(f"There are {null_meta_data_records} records with NULL meta_data.")

    message = f"""
Added license_url to *{item_count}* items`
Now, there are {null_meta_data_records} records with NULL meta_data.
"""
    send_message(
        message,
        username="Airflow DAG Data Normalization - license_url",
        dag_id=DAG_ID,
    )

    logger.info(message)


DAG_DEFAULT_ARGS = {
    "owner": "data-eng-admin",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 15),
    "template_undefined": jinja2.Undefined,
    "email_on_retry": False,
    "schedule_interval": None,
    "retries": 0,
    "on_failure_callback": slack.on_failure_callback,
}


dag = DAG(
    dag_id=DAG_ID,
    default_args=DAG_DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    # Use the docstring at the top of the file as md docs in the UI
    doc_md=__doc__,
    tags=["data_normalization"],
)

with dag:
    get_statistics = BranchPythonOperator(
        task_id="get_stats",
        python_callable=get_statistics,
        op_kwargs={"postgres_conn_id": POSTGRES_CONN_ID},
    )
    update_license_url = PythonOperator(
        task_id=UPDATE_LICENSE_URL,
        python_callable=update_license_url,
        op_kwargs={"postgres_conn_id": POSTGRES_CONN_ID},
    )
    final_report = PythonOperator(
        task_id=FINAL_REPORT,
        python_callable=final_report,
        op_kwargs={
            "postgres_conn_id": POSTGRES_CONN_ID,
            "item_count": XCOM_PULL_TEMPLATE.format(
                update_license_url.task_id, "return_value"
            ),
        },
    )

    get_statistics >> [update_license_url, final_report]
    update_license_url >> final_report

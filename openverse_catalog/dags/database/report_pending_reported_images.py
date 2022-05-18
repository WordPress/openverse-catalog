"""
# Report Pending Reported Images DAG
This DAG checks for any user-reported images pending manual review, and alerts
via Slack.

An image may be reported for mature content or copyright infringement, for
example. Once reported, these require manual review through the Django Admin to
determine whether further action (such as deindexing the record) needs to be
taken. If an image has been reported multiple times, it only needs to be
reviewed once and so is only counted once in the reporting by this DAG.

The DAG should be run weekly.
"""
import logging
import os
from textwrap import dedent
from urllib.parse import urljoin

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from common import slack
from common.constants import DAG_DEFAULT_ARGS, XCOM_PULL_TEMPLATE


logger = logging.getLogger(__name__)

DAG_ID = "report_pending_reported_images"
DB_CONN_ID = os.getenv("OPENLEDGER_API_CONN_ID", "postgres_openledger_api")

IMAGE_REPORTS_TABLE = "nsfw_reports"

# Column name constants
IMAGE_URL = "identifier"
STATUS = "status"


def get_distinct_pending_image_reports(db_conn_id, ti):
    postgres = PostgresHook(postgres_conn_id=db_conn_id)

    nsfw_reports = dedent(
        f"""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT {IMAGE_URL}
            FROM {IMAGE_REPORTS_TABLE}
            WHERE {STATUS}='pending_review'
        ) AS distinct_reports;
        """
    )
    distinct_report_count = postgres.get_records(nsfw_reports)[0][0]
    logger.info(f"{distinct_report_count} distinct images require manual review")

    return distinct_report_count


def report_actionable_records(distinct_report_count):
    if distinct_report_count == 0:
        slack.send_message(
            "No images require review at this time :tada:",
            username="Reported Images Check-In",
        )
        return

    admin_url = os.getenv("DJANGO_ADMIN_URL", "http://localhost:8000/admin")
    image_review_link = urljoin(
        admin_url, "api/imagereport/?status__exact=pending_review"
    )

    message = dedent(
        f"""
        {distinct_report_count} reported images pending review.
        Manually review reports <{image_review_link}|here>.
        """
    )

    logger.info(message)
    slack.send_message(message, username="Reported Images Ready For Review")


def create_dag():
    dag = DAG(
        dag_id=DAG_ID,
        default_args=DAG_DEFAULT_ARGS,
        schedule_interval="@weekly",
        catchup=False,
        tags=["database"],
        doc_md=__doc__,
    )

    with dag:
        get_image_reports = PythonOperator(
            task_id="get_distinct_pending_image_reports",
            python_callable=get_distinct_pending_image_reports,
            op_kwargs={"db_conn_id": DB_CONN_ID},
        )

        report_pending_reports = PythonOperator(
            task_id="report_pending_image_reports",
            python_callable=report_actionable_records,
            op_kwargs={
                "distinct_report_count": XCOM_PULL_TEMPLATE.format(
                    get_image_reports.task_id, "return_value"
                )
            },
        )

        get_image_reports >> report_pending_reports

    return dag


globals()[DAG_ID] = create_dag()

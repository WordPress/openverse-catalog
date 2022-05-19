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
from common.constants import DAG_DEFAULT_ARGS, MEDIA_TYPES, XCOM_PULL_TEMPLATE


logger = logging.getLogger(__name__)

DAG_ID = "report_pending_reported_media"
DB_CONN_ID = os.getenv("OPENLEDGER_API_CONN_ID", "postgres_openledger_api")

REPORTS_TABLES = {
    "image": "nsfw_reports",
    "audio": "nsfw_reports_audio",
}

IMAGE_REPORTS_TABLE = "nsfw_reports"
AUDIO_REPORTS_TABLE = "nsfw_reports_audio"

# Column name constants
URL = "identifier"
STATUS = "status"


def get_distinct_pending_reports(db_conn_id, media_type, ti):
    postgres = PostgresHook(postgres_conn_id=db_conn_id)

    nsfw_reports = dedent(
        f"""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT {URL}
            FROM {REPORTS_TABLES[media_type]}
            WHERE {STATUS}='pending_review'
        ) AS distinct_reports;
        """
    )
    distinct_report_count = postgres.get_records(nsfw_reports)[0][0]
    logger.info(f"{distinct_report_count} distinct records require manual review")

    return distinct_report_count


def report_actionable_records(report_counts_by_media_type):
    if all(value == 0 for value in report_counts_by_media_type.values()):
        slack.send_message(
            "No records require review at this time :tada:",
            username="Reported Media Check-In",
        )
        return

    admin_url = os.getenv("DJANGO_ADMIN_URL", "http://localhost:8000/admin")
    media_type_reports = ""

    for media_type, distinct_report_count in report_counts_by_media_type.items():
        if distinct_report_count == 0:
            continue

        admin_review_link = urljoin(
            admin_url, f"api/{media_type}report/?status__exact=pending_review"
        )

        media_type_reports += (
            f"  - <{admin_review_link}|{media_type}>: {distinct_report_count}"
        )
        media_type_reports += "\n"

    message = dedent(
        f"""
        The following media have been reported and require manual review:
        {media_type_reports}
        """
    )
    logger.info(message)
    slack.send_message(message, username="Reported Media Requires Review")


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
        get_reports_tasks = []
        report_counts_by_media_type = {}

        for media_type in MEDIA_TYPES:
            get_reports = PythonOperator(
                task_id=f"get_pending_{media_type}_reports",
                python_callable=get_distinct_pending_reports,
                op_kwargs={"db_conn_id": DB_CONN_ID, "media_type": media_type},
            )

            report_counts_by_media_type[media_type] = XCOM_PULL_TEMPLATE.format(
                get_reports.task_id, "return_value"
            )
            get_reports_tasks.append(get_reports)

        report_pending_reports = PythonOperator(
            task_id="report_pending_media_reports",
            python_callable=report_actionable_records,
            op_kwargs={"report_counts_by_media_type": report_counts_by_media_type},
        )

        get_reports_tasks >> report_pending_reports

    return dag


globals()[DAG_ID] = create_dag()

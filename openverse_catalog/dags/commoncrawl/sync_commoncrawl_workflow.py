import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from common.tsv_cleaner import clean_tsv_directory


airflowHome = os.environ["AIRFLOW_HOME"]

DAG_DEFAULT_ARGS = {
    "owner": "data-eng-admin",
    "depends_on_past": False,
    "start_date": datetime(2020, 1, 15),
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(days=1),
}

DAG_ID = "sync_commoncrawl_workflow"

DEFAULT_OUTPUT_DIR = "/tmp"
TSV_SUBDIR = "common_crawl_tsvs/"

CRAWL_OUTPUT_DIR = os.path.join(
    os.environ.get("OUTPUT_DIR", DEFAULT_OUTPUT_DIR), TSV_SUBDIR
)


def _empty_tsv_dir(tsv_directory):
    for tsv in os.listdir(tsv_directory):
        os.remove(os.path.join(tsv_directory, tsv))


def create_dag():
    dag = DAG(
        dag_id=DAG_ID,
        default_args=DAG_DEFAULT_ARGS,
        start_date=datetime(2020, 1, 15),
        schedule_interval="0 16 15 * *",
        catchup=False,
        tags=["commoncrawl"],
    )

    with dag:
        create_dir_task = PythonOperator(
            task_id="create_tsv_directory",
            python_callable=os.makedirs,
            op_args=[CRAWL_OUTPUT_DIR],
            op_kwargs={"exist_ok": True},
            depends_on_past=False,
        )
        sync_tsvs_task = BashOperator(
            task_id="sync_commoncrawl_workflow",
            bash_command=(
                f"python {airflowHome}/dags/commoncrawl_s3_syncer/SyncImageProviders.py"
            ),
            env={
                "S3_BUCKET": os.environ["S3_BUCKET"],
                "OUTPUT_DIR": CRAWL_OUTPUT_DIR,
                "AWS_ACCESS_KEY": os.environ["AWS_ACCESS_KEY"],
                "AWS_SECRET_KEY": os.environ["AWS_SECRET_KEY"],
            },
        )
        clean_tsvs_task = PythonOperator(
            task_id="clean_commoncrawl_tsvs",
            python_callable=clean_tsv_directory,
            op_args=[CRAWL_OUTPUT_DIR],
            depends_on_past=False,
        )
        empty_dir_task = PythonOperator(
            task_id="empty_tsv_directory",
            python_callable=_empty_tsv_dir,
            op_args=[CRAWL_OUTPUT_DIR],
            depends_on_past=False,
        )

        (create_dir_task >> sync_tsvs_task >> clean_tsvs_task >> empty_dir_task)

    return dag


globals()[DAG_ID] = create_dag()

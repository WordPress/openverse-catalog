import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from common.constants import POSTGRES_CONN_ID
from common.sql_helpers import PGExecuteQueryOperator, PostgresHook


logger = logging.getLogger(__name__)


TEST_SQL = "SELECT PG_SLEEP(1);"


def timed_pg_hook_sleeper(
    ti: TaskInstance,
    statement_timeout: float = None,
):
    logger.info(f"{PostgresHook.get_execution_timeout(ti)=}, {statement_timeout=}")
    pg = PostgresHook(
        default_statement_timeout=PostgresHook.get_execution_timeout(ti),
        postgres_conn_id=POSTGRES_CONN_ID,
    )
    pg.run(sql=TEST_SQL, statement_timeout=statement_timeout)


def create_pg_timeout_tester_dag():
    with DAG(
        dag_id="a_pg_timeout_tester",
        schedule=None,
        doc_md="DAG to test query timeouts in postgres",
        start_date=datetime(2023, 1, 1),
    ) as dag:
        pg_operator_happy = PGExecuteQueryOperator(
            task_id="pg_operator_happy",
            retries=0,
            conn_id=POSTGRES_CONN_ID,
            sql=TEST_SQL,
            execution_timeout=timedelta(seconds=2),
            doc_md="Custom PG operator, with query finished before execution timeout",
        )
        pg_hook_happy = PythonOperator(
            task_id="pg_hook_happy",
            retries=0,
            python_callable=timed_pg_hook_sleeper,
            execution_timeout=timedelta(hours=2),
            doc_md="Custom PG hook, with query finished before execution timeout",
        )
        pg_hook_no_timeout = PythonOperator(
            task_id="pg_hook_no_timeout",
            retries=0,
            python_callable=timed_pg_hook_sleeper,
            doc_md="Custom PG hook, with no execution timeout",
        )
        [pg_operator_happy, pg_hook_happy, pg_hook_no_timeout]
    return dag

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from common.constants import POSTGRES_CONN_ID
from common.sql_helpers import TimedPostgresHook, pg_runtime_args


# Revised understanding of the issue, based on more extreme test times below.
#   Go to the local airflow interface (http://localhost:9090/home) and run this dag.
#   Look at the log timestamps and observe that Airflow just waits for the db job to
#      finish. It's not even that Airflow marks the task done and leaves the db job
#      running, it's that both are running long after the execution timeout, unless we
#      proactively tell the database to kill the job after the execution timeout has
#      passed.
#   Check the basic / overall database timeout: `show statement_timeout;` (it should be
#      0, which means no timeout at all.)


logger = logging.getLogger(__name__)

# SQL to test execution timeouts
EXECUTION_TIMEOUT = timedelta(seconds=5)
MAPPING_MULTIPLIERS = [0.2, 1, 12]
SLEEP_SQL = "SELECT PG_SLEEP({}), CURRENT_TIMESTAMP;"


def get_sql(
    sleep_timer: timedelta,
    ti: TaskInstance,
    set_timeout: bool = False,
):
    """
    Use functions from sql_helpers and templates above to generate standard
    sqls for testing under different timing and pg connection tool scenarios.
    Currently set-up to exclude database timeout settings to demonstrate which
    tools correctly handle timeouts and which don't.
    """
    sleep_sql = SLEEP_SQL.format(sleep_timer.total_seconds())
    if set_timeout:
        runtime_param_SQL = pg_runtime_args(ti)
    else:
        runtime_param_SQL = "; ".join(
            [
                statement
                for statement in pg_runtime_args(ti).split(";")
                if "SET statement_timeout TO" not in statement
            ]
        )
    logger.info(f"{ti.task.execution_timeout=}, {sleep_timer=}, {runtime_param_SQL=}")
    return runtime_param_SQL + sleep_sql


def hook_sleeper(
    sleep_timer: timedelta,
    ti: TaskInstance,
    set_timeout: bool = False,
    postgres_conn_id=POSTGRES_CONN_ID,
):
    sql = get_sql(sleep_timer=sleep_timer, ti=ti, set_timeout=set_timeout)
    pg = PostgresHook(postgres_conn_id)
    return pg.get_records(sql)


def timed_hook_sleeper(
    sleep_timer: timedelta,
    ti: TaskInstance,
    postgres_conn_id=POSTGRES_CONN_ID,
):
    sql = get_sql(sleep_timer=sleep_timer, ti=ti, set_timeout=False)
    pg = TimedPostgresHook(ti, postgres_conn_id)
    return pg.get_records(sql)


with DAG(
    "a_pg_fail_tester",
    start_date=datetime(2022, 9, 1),
    schedule=None,
    max_active_tasks=1,
    doc_md="DAG to test query timeouts in postgres",
) as dag:

    with TaskGroup(group_id="finish_late") as finish_late:
        sleep_for = EXECUTION_TIMEOUT * MAPPING_MULTIPLIERS[2]
        sql_operator_late = SQLExecuteQueryOperator(
            task_id="sql_operator_late",
            retries=0,
            conn_id=POSTGRES_CONN_ID,
            sql=SLEEP_SQL.format(sleep_for.total_seconds()),
            execution_timeout=EXECUTION_TIMEOUT,
            trigger_rule="all_done",
            doc_md="SQL operator, with query set to finish after execution timeout",
        )
        pg_hook_late = PythonOperator(
            task_id="pg_hook_late",
            retries=0,
            python_callable=hook_sleeper,
            execution_timeout=EXECUTION_TIMEOUT,
            trigger_rule="all_done",
            op_kwargs={
                "sleep_timer": sleep_for,
            },
            doc_md="PostgresHook, with query set to finish after execution timeout",
        )
        timed_hook_late = PythonOperator(
            task_id="timed_hook_late",
            retries=0,
            python_callable=timed_hook_sleeper,
            execution_timeout=EXECUTION_TIMEOUT,
            trigger_rule="all_done",
            op_kwargs={
                "sleep_timer": sleep_for,
            },
            doc_md="TimedPostgresHook, with query set to finish after execution timeout",
        )

    with TaskGroup(group_id="finish_on_time") as finish_on_time:
        sleep_for = EXECUTION_TIMEOUT * MAPPING_MULTIPLIERS[0]
        sql_operator_ok = SQLExecuteQueryOperator(
            task_id="sql_operator_ok",
            retries=0,
            conn_id=POSTGRES_CONN_ID,
            sql=SLEEP_SQL.format(sleep_for.total_seconds()),
            execution_timeout=EXECUTION_TIMEOUT,
            trigger_rule="all_done",
            doc_md="SQL operator, with query set to finish before execution timeout",
        )
        pg_hook_ok = PythonOperator(
            task_id="pg_hook_ok",
            retries=0,
            python_callable=hook_sleeper,
            execution_timeout=EXECUTION_TIMEOUT,
            trigger_rule="all_done",
            op_kwargs={
                "sleep_timer": sleep_for,
            },
            doc_md="PostgresHook, with query set to finish before execution timeout",
        )
        timed_hook_ok = PythonOperator(
            task_id="timed_hook_ok",
            retries=0,
            python_callable=timed_hook_sleeper,
            execution_timeout=EXECUTION_TIMEOUT,
            trigger_rule="all_done",
            op_kwargs={
                "sleep_timer": sleep_for,
            },
            doc_md="TimedPostgresHook, with query set to finish before execution timeout",
        )

    # mapped_sleeper = PythonOperator.partial(
    #     task_id="pg_hook_mapped",
    #     retries=0,
    #     python_callable=hook_sleeper,
    #     execution_timeout=EXECUTION_TIMEOUT,
    #     trigger_rule="all_done",
    #     doc_md="PostgresHook, with mapped sleep times, to test context parameters",
    # ).expand(op_args=[[EXECUTION_TIMEOUT * m] for m in reversed(MAPPING_MULTIPLIERS)])

    sleep_for = EXECUTION_TIMEOUT * MAPPING_MULTIPLIERS[2]
    pg_hook_late_with_timeout = PythonOperator(
        task_id="pg_hook_late_with_timeout",
        retries=0,
        python_callable=hook_sleeper,
        execution_timeout=EXECUTION_TIMEOUT,
        trigger_rule="all_done",
        op_kwargs={
            "sleep_timer": sleep_for,
            "set_timeout": True,
        },
        doc_md="PostgresHook, with long-running query and db-timeout set",
    )

    [finish_late, finish_on_time, pg_hook_late_with_timeout]

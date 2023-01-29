import logging
import os
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from common.constants import POSTGRES_CONN_ID
from common.sql_helpers import pg_runtime_args


# To satisfy yourself that we don't have to worry about orphaned DB tasks...
#   Go to the local airflow interface (http://localhost:9090/home) and run this dag.
#   Go to the local postgres, with `just db-shell`.
#   Check the basic / overall database timeout: `show statement_timeout;` (it should be
#      0, which means no timeout at all.)
#   Check that each task only has one running process record, even though we updated the
#      the tracking table at the start of each step, and would expect (if timeouts were
#      not being handled) that we would see the long running tasks with multiple
#      recorde.
#      ```
#      select status_as_of, query_age, application_name, usename, state, query
#      from db_job_status
#      where query_age>'10s'
#      order by query_age desc;
#      select query_start, status_as_of, application_name, query_age, state
#      from db_job_status
#      where length(application_name)>5
#      order by status_as_of;
#      ```


PG_HOOK_CONN_ID = POSTGRES_CONN_ID
PSYCOPG2_CONN_ID = os.getenv("AIRFLOW_CONN_POSTGRES_OPENLEDGER_TESTING")

logger = logging.getLogger(__name__)

# SQL to capture state of the system at various points during the dag run.
STATUS_SQL = (
    "SELECT clock_timestamp() AT TIME ZONE 'America/New_York' status_as_of, "
    "query_start AT TIME ZONE 'America/New_York' query_start, "
    "age(clock_timestamp(), query_start) query_age, pid, usename, "
    "application_name, query, state "
    "FROM pg_stat_activity "
    "WHERE (query != '<IDLE>' AND query NOT ILIKE '%pg_stat_activity%') "
    "OR application_name != 'pgcli'"
)
CREATE_STATUS_TABLE_SQL = (
    "SET application_name TO 'initial_status_before_tests';\n"
    f"CREATE TABLE IF NOT EXISTS db_job_status AS ({STATUS_SQL});\n commit;\n"
)
CAPTURE_STATUS_SQL = f"INSERT INTO db_job_status ({STATUS_SQL});"

# SQL to test execution timeouts
EXECUTION_TIMEOUT = timedelta(seconds=10)
MAPPING_MULTIPLIERS = [0.25, 1, 2]
SLEEP_SQL = "SELECT PG_SLEEP({}), CURRENT_TIMESTAMP;"


def get_sql(
    sleep_timer: timedelta,
    context: dict | TaskInstance | None = None,
    set_timeout: bool = False,
    execution_timeout: timedelta = EXECUTION_TIMEOUT,
):
    """
    Use functions from sql_helpers and templates above to generate standard
    sqls for testing under different timing and pg connection tool scenarios.
    Currently set-up to exclude database timeout settings to demonstrate which
    tools correctly handle timeouts and which don't.
    """
    sleep_sql = SLEEP_SQL.format(sleep_timer.total_seconds())
    runtime_param_SQL = ""
    if context and set_timeout:
        runtime_param_SQL = pg_runtime_args(context)
    elif context:
        runtime_param_SQL = ";".join(
            [
                statement
                for statement in pg_runtime_args(context).split(";")
                if "SET statement_timeout TO" not in statement
            ]
        )
    else:
        runtime_param_SQL = "SET application_name TO 'get_sql_call_without_context'; \n"
    logger.info(f"{execution_timeout=}, {sleep_timer=}, {runtime_param_SQL=}")
    return runtime_param_SQL + CAPTURE_STATUS_SQL + sleep_sql


def hook_sleeper(
    sleep_timer: timedelta,
    ti: TaskInstance | dict,
    set_timeout: bool = False,
    execution_timeout: timedelta = EXECUTION_TIMEOUT,
    postgres_conn_id=PG_HOOK_CONN_ID,
):
    sql = get_sql(
        sleep_timer=sleep_timer,
        context=ti,
        execution_timeout=execution_timeout,
        set_timeout=set_timeout,
    )
    pg = PostgresHook(postgres_conn_id)
    return pg.run(sql)


def psycopg2_sleeper(
    sleep_timer: timedelta,
    ti: TaskInstance | dict,
    set_timeout: bool = False,
    execution_timeout: timedelta = EXECUTION_TIMEOUT,
    postgres_conn_id=PSYCOPG2_CONN_ID,
):
    sql = get_sql(
        sleep_timer=sleep_timer,
        context=ti,
        execution_timeout=execution_timeout,
        set_timeout=set_timeout,
    )
    conn = psycopg2.connect(postgres_conn_id)
    cur = conn.cursor()
    results = cur.execute(sql)
    conn.commit()
    return results


with DAG(
    "a_pg_fail_tester",
    start_date=datetime(2022, 9, 1),
    schedule=None,
    max_active_tasks=1,
    doc_md="DAG to test query timeouts in postgres",
) as dag:

    create_tracker_table = SQLExecuteQueryOperator(
        task_id="create_tracker_table",
        retries=0,
        conn_id=POSTGRES_CONN_ID,
        sql=CREATE_STATUS_TABLE_SQL,
        doc_md="Create table db_job_status and populate with initial status",
    )

    with TaskGroup(group_id="finish_late") as finish_late:
        sleep_for = EXECUTION_TIMEOUT * MAPPING_MULTIPLIERS[2]
        sql_operator_late = SQLExecuteQueryOperator(
            task_id="sql_operator_late",
            retries=0,
            conn_id=POSTGRES_CONN_ID,
            # can't figure out how to pass the task_instance to the sql generator here.
            sql=get_sql(sleep_timer=sleep_for),
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
        psycopg2_late = PythonOperator(
            task_id="psycopg2_late",
            retries=0,
            python_callable=psycopg2_sleeper,
            execution_timeout=EXECUTION_TIMEOUT,
            trigger_rule="all_done",
            op_kwargs={
                "sleep_timer": sleep_for,
            },
            doc_md="psycopg2, with query set to finish after execution timeout",
        )

    with TaskGroup(group_id="finish_on_time") as finish_on_time:
        sleep_for = EXECUTION_TIMEOUT * MAPPING_MULTIPLIERS[0]
        sql_operator_ok = SQLExecuteQueryOperator(
            task_id="sql_operator_ok",
            retries=0,
            conn_id=POSTGRES_CONN_ID,
            sql=get_sql(sleep_timer=sleep_for),
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
        psycopg2_ok = PythonOperator(
            task_id="psycopg2_ok",
            retries=0,
            python_callable=psycopg2_sleeper,
            execution_timeout=EXECUTION_TIMEOUT,
            trigger_rule="all_done",
            op_kwargs={
                "sleep_timer": sleep_for,
            },
            doc_md="psycopg2, with query set to finish before execution timeout",
        )

    mapped_sleeper = PythonOperator.partial(
        task_id="pg_hook_mapped",
        retries=0,
        python_callable=hook_sleeper,
        execution_timeout=EXECUTION_TIMEOUT,
        trigger_rule="all_done",
        doc_md="PostgresHook, with mapped sleep times, to test context parameters",
    ).expand(op_args=[[EXECUTION_TIMEOUT * m] for m in reversed(MAPPING_MULTIPLIERS)])

    with TaskGroup(group_id="finish_late_with_timeout") as finish_late_with_timeout:
        sleep_for = EXECUTION_TIMEOUT * MAPPING_MULTIPLIERS[2]
        sql_operator_late_with_timeout = SQLExecuteQueryOperator(
            task_id="sql_operator_late_with_timeout",
            retries=0,
            conn_id=POSTGRES_CONN_ID,
            # can't figure out how to pass the task_instance to the sql generator here.
            sql=get_sql(sleep_timer=sleep_for, set_timeout=True),
            execution_timeout=EXECUTION_TIMEOUT,
            trigger_rule="all_done",
            doc_md="SQL operator, with long-running query and db-timeout set",
        )
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
        psycopg2_late_with_timeout = PythonOperator(
            task_id="psycopg2_late_with_timeout",
            retries=0,
            python_callable=psycopg2_sleeper,
            execution_timeout=EXECUTION_TIMEOUT,
            trigger_rule="all_done",
            op_kwargs={
                "sleep_timer": sleep_for,
                "set_timeout": True,
            },
            doc_md="psycopg2, with long-running query and db-timeout set",
        )

    (
        create_tracker_table
        >> finish_late
        >> finish_on_time
        >> mapped_sleeper
        >> finish_late_with_timeout
    )

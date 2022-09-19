import logging
import os
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from common.constants import POSTGRES_CONN_ID


PSYCOPG2_CONN_ID = os.getenv("AIRFLOW_CONN_POSTGRES_OPENLEDGER_TESTING")

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# SET UP TEST SQLS -- PLAY AROUND WITH SECONDS IN EXECUTION_TIMEOUT AND SLEEP_SQL TO
# SEE HOW THE SYSTEM PERFORMS WHEN WE FORCE SOMETHING TO TIMEOUT.
EXECUTION_TIMEOUT = timedelta(seconds=3)
SLEEP_SQL = "SELECT PG_SLEEP(30), CURRENT_TIMESTAMP;"

# SQL TO CAPTURE STATE OF THE SYSTEM AT POINTS DURING TESTING.
STATUS_SQL = (
    "SELECT clock_timestamp() AT TIME ZONE 'America/New_York' status_as_of, "
    + "query_start, age(clock_timestamp(), query_start) query_age, pid, usename, "
    + "application_name, query, state "
    + "FROM pg_stat_activity WHERE 1=1 "
)
CREATE_STATUS_TABLE_SQL = (
    f"CREATE TABLE IF NOT EXISTS db_job_status AS ({STATUS_SQL} LIMIT 0);\n commit;\n"
)
CAPTURE_STATUS_SQL = f"INSERT INTO db_job_status ({STATUS_SQL});\n commit;\n"


# DEFINE FUNCTION FOR PREPEND SQL TO SET TIMEOUT AND APPLICATION NAME
# TO DO: FIGURE OUT IF/HOW TO PASS EXECUTION TIME OUT FROM AIRFLOW
def pg_runtime_args(
    task_instance_key_str: str,
    ts_nodash: str,
    execution_timeout: timedelta,
):
    """
    Creates SQL statements to prepend to any Airflow SQL that identifies the job with
    a particular DAG, task, and (if possible) specific execution of the task, and sets
    the database statement_timeout to match the airflow execution_timeout.

    task_instance_key_str: unique identifier for a specific run of a specific task, as
    long as it is only run once per day.
    ts_nodash: to append a timestamp to the above to identify an execution within a day.
    execution_timeout:
    """
    # check that Airflow jinja is being parsed, raise an error if not.
    if "ts_nodash" in ts_nodash or "task_instance_key_str" in task_instance_key_str:
        raise ValueError(f"No jinja parsing: {ts_nodash=}, {task_instance_key_str=}")
    db_settings = dict()
    # application_name to identify job if we need to terminate it, particularly for
    # failures that are not from timing out. the unique identifier from airflow only
    # counts daily runs so adding timestamp.
    ts_time_only = ts_nodash[8:]
    db_settings["application_name"] = f"{task_instance_key_str}{ts_time_only}"
    # statement_timeout, to set a timeout on the database end.
    if issubclass(type(execution_timeout), timedelta):
        db_settings["statement_timeout"] = f"'{execution_timeout.total_seconds()}s'"
    else:
        raise TypeError(
            f"execution_timeout is {type(execution_timeout)}, not timedelta"
        )
    # this is based on how the operator works, minus protection from SQL injection:
    # https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/_modules/airflow/providers/postgres/operators/postgres.html
    # TO DO: add the SQL templating that Airflow uses
    return (
        "\n".join([f"SET {key} TO {value};" for (key, value) in db_settings.items()])
        + "\n"
    )


# TO DO: DEFINE FUNCTION FOR POSTHOC JOB TERMINATION IN THE DATABASE

# def pg_terminator(application_name: str, postgres_conn_id):
#     logger.info(
#         f"TERMINATOR: Checking for processes running under {application_name}."
#     )
#     pg = PostgresHook(postgres_conn_id)
#     db_response = pg.get_records(
#         f"SELECT pid FROM pg_stat_activity WHERE application_name='{application_name}';"
#     )
#     pid_list = [p[0] for p in db_response]
#     if pid_list:
#         logger.info(f"Terminating these processes: {pid_list}")
#         for pid in pid_list:
#             pg.run(f"SELECT pg_terminate_backend({pid});")
#     else:
#         logger.info(f"No running processes found. ({db_response})")

# DEFINE PYTHON CALLABLES TO TEST ABOVE WITH POSTGRES HOOK AND PSYCOPG2


def hook_sleeper(
    task_instance_key_str: str,
    ts_nodash: str,
    execution_timeout: timedelta,
    postgres_conn_id=POSTGRES_CONN_ID,
):
    pg = PostgresHook(postgres_conn_id)
    runtime_param_SQL = pg_runtime_args(
        task_instance_key_str, ts_nodash, execution_timeout
    )
    logger.info(f"{runtime_param_SQL=}")
    sql_result = pg.run(runtime_param_SQL + SLEEP_SQL + CAPTURE_STATUS_SQL)
    return sql_result


def psycopg2_sleeper(
    task_instance_key_str: str,
    ts_nodash: str,
    execution_timeout: timedelta,
    postgres_conn_id=PSYCOPG2_CONN_ID,
):
    db_params = pg_runtime_args(task_instance_key_str, ts_nodash, execution_timeout)
    logger.info(f"Create the connection with {db_params=}")
    conn = psycopg2.connect(PSYCOPG2_CONN_ID)
    cur = conn.cursor()
    cur.execute(db_params + SLEEP_SQL + CAPTURE_STATUS_SQL)
    conn.commit()


# DEFINE DAG TO RUN EACH OF THE OPERATIONS ABOVE FOR TESTING

with DAG(
    "a_pg_fail_tester",
    start_date=datetime(2022, 9, 1),
    schedule_interval=None,
    max_active_tasks=3,
) as dag:

    get_db_status_before = PostgresOperator(
        task_id="get_db_status_before",
        retries=0,
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=CREATE_STATUS_TABLE_SQL + CAPTURE_STATUS_SQL,
    )

    pg_operator_sleep = PostgresOperator(
        task_id="pg_operator_sleep",
        retries=0,
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=SLEEP_SQL + CAPTURE_STATUS_SQL,
        execution_timeout=EXECUTION_TIMEOUT,
        # PostgresOperator won't parse jinja for sql, params, or runtime_parameters
        runtime_parameters={
            "application_name": "a_pg_fail_tester__pg_operator_sleep__"
            + str(datetime.now()),
            "statement_timeout": f"{EXECUTION_TIMEOUT.total_seconds()}s",
        },
    )

    pg_hook_sleep = PythonOperator(
        task_id="pg_hook_sleep",
        retries=0,
        python_callable=hook_sleeper,
        execution_timeout=EXECUTION_TIMEOUT,
        op_kwargs={
            "task_instance_key_str": "{{ task_instance_key_str }}",
            "ts_nodash": "{{ ts_nodash }}",
            "execution_timeout": EXECUTION_TIMEOUT,
        },
    )

    psycopg2_sleep = PythonOperator(
        task_id="psycopg2_sleep",
        retries=0,
        python_callable=psycopg2_sleeper,
        execution_timeout=EXECUTION_TIMEOUT,
        op_kwargs={
            "task_instance_key_str": "{{ task_instance_key_str }}",
            "ts_nodash": "{{ ts_nodash }}",
            "execution_timeout": EXECUTION_TIMEOUT,
        },
    )

    get_db_status_after = PostgresOperator(
        task_id="get_db_status_after",
        retries=0,
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=CAPTURE_STATUS_SQL,
        trigger_rule="all_done",
    )

    (
        get_db_status_before
        >> [pg_operator_sleep, pg_hook_sleep, psycopg2_sleep]
        >> get_db_status_after
    )

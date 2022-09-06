import logging
import psycopg2
import pytest

from common.constants import POSTGRES_CONN_ID
from common.on_failure_callback import *
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

PSYCOPG_CONNECTION_ID = os.getenv("AIRFLOW_CONN_POSTGRES_OPENLEDGER_TESTING")
SLEEP_SQL_TEMPLATE = "SELECT PG_SLEEP({sleep_time});"
logger = logging.getLogger(__name__)

@pytest.mark.parametrize(
    "context, expected",
    [
        pytest.param("app_ID_value", "app_ID_value", id="happy_path_str"),
        pytest.param({"task_instance_key_str": "app_ID_value"}, "app_ID_value", id="happy_path_dict"),
    ],
)
def test_get_task_app_name(context, expected):
    actual = get_task_app_name(context)
    assert actual == expected
    
@pytest.mark.parametrize(
    "context",
    [
        pytest.param(None, id="None"),
        pytest.param(1, id="int"),
    ],
)
def test_get_task_app_name_type_error(context):
    with pytest.raises(TypeError):
        get_task_app_name(context)

def test_get_task_app_name_index_error():
    context = {"exception": "raise exception"}
    with pytest.raises(IndexError):
        get_task_app_name(context)

# # TO DO: Add tests for pg_terminator and integrated

# # MORE TESTING: 
# # I used the code commented out below to try to recreate the issue from airflow and
# # check that things were deleted.
# # To manually check what was left running, I used just db-shell and this SQL:
# # select pid, application_name, left(query, 50), backend_start from pg_stat_activity order by backend_start;

# # DAG FOR TESTING:

# def hook_sleeper(
#     task_app_name,
#     postgres_conn_id=POSTGRES_CONN_ID,
#     sleep_time=30,
# ):
#     pg = PostgresHook(postgres_conn_id)
#     sql_string = (
#         PG_SET_NAME_TEMPLATE.format(task_app_name) 
#         + SLEEP_SQL_TEMPLATE.format(sleep_time = sleep_time)
#     )
#     logger.info(f"Running this SQL: {sql_string}")
#     sql_result = pg.run(sql_string)

# def psycopg2_sleeper(
#     task_app_name,
#     postgres_conn_id=PSYCOPG_CONNECTION_ID,
#     sleep_time=30,
# ):
#     conn = psycopg2.connect(postgres_conn_id)
#     cur = conn.cursor()
#     sql_string = (
#         PG_SET_NAME_TEMPLATE.format(task_app_name) 
#         + SLEEP_SQL_TEMPLATE.format(sleep_time = sleep_time)
#     )
#     logger.info(f"Running this SQL: {sql_string}")
#     cur.execute(sql_string)
#     conn.commit()

# with DAG("killer_pg_test", 
#     start_date=datetime(2022, 9, 4),
#     schedule_interval=None,
# ) as dag:

#     create_tracking_table = PostgresOperator(
#         task_id = "create_tracking_tables",
#         postgres_conn_id = POSTGRES_CONN_ID,
#         sql = (
#             PG_SET_NAME_TEMPLATE.format(get_task_app_name({{ task_instance_key_str }})) 
#             + "CREATE TABLE IF NOT EXISTS db_job_status AS ("
#             + "SELECT now() as check_timestamp, pid, usename, application_name, backend_start, query_start, query, state "
#             + "FROM pg_stat_activity LIMIT 0"
#             + "); \n"
#             + "commit;"
#         ),
#         retries = 0,
#     )

#     pg_operator_sleep_30 = PostgresOperator(
#         task_id = "pg_operator_sleep_30",
#         postgres_conn_id = POSTGRES_CONN_ID,
#         sql = (
#             PG_SET_NAME_TEMPLATE.format(get_task_app_name({{ task_instance_key_str }})) 
#             + SLEEP_SQL_TEMPLATE.format(sleep_time = 30)
#         ),
#         retries = 0,
#         execution_timeout = timedelta(seconds=5),
#         on_failure_callback = integrated,
#     )

#     pg_hook_sleep_30 = PythonOperator(
#         task_id = "pg_hook_sleep_30",
#         python_callable = hook_sleeper,
#         op_kwargs = {"task_app_name": get_task_app_name({{ task_instance_key_str }})},
#         retries = 0,
#         execution_timeout = timedelta(seconds=5),
#         on_failure_callback = integrated,
#     )

#     psycopg2_sleep_30 = PythonOperator(
#         task_id = "psycopg2_sleep_30",
#         python_callable = psycopg2_sleeper,
#         op_kwargs = {"task_app_name": get_task_app_name({{ task_instance_key_str }})},
#         retries = 0,
#         execution_timeout = timedelta(seconds=5),
#         on_failure_callback = integrated,
#     )

#     get_db_status = PostgresOperator(
#         task_id = "get_db_status",
#         postgres_conn_id = POSTGRES_CONN_ID,
#         sql = (
#             PG_SET_NAME_TEMPLATE.format(get_task_app_name({{ task_instance_key_str }})) 
#             + "INSERT INTO db_job_status ("
#             + "SELECT now() as check_timestamp, pid, usename, application_name, backend_start, query_start, query, state "
#             + "FROM pg_stat_activity WHERE query != '<IDLE>' AND query NOT ILIKE '%pg_stat_activity%' "
#             + "); \n"
#             + "commit;"
#         ),
#         retries = 0,
#         trigger_rule = 'all_done',
#     )

#     (
#         create_tracking_table
#         >> 
#         [
#             pg_operator_sleep_30, 
#             pg_hook_sleep_30,
#             psycopg2_sleep_30
#         ] 
#         >> 
#         get_db_status
#     )

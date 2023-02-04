import logging
from datetime import timedelta

import pytest
from airflow.models.taskinstance import TaskInstance
from common.constants import POSTGRES_CONN_ID
from common.sql_helpers import PGExecuteQueryOperator, PostgresHook
from psycopg2.errors import QueryCanceled

from tests.dags.common.test_resources.dags.test_timeout_pg import (
    TEST_SQL,
    create_pg_timeout_tester_dag,
)


logger = logging.getLogger(__name__)

HAPPY_DAG = create_pg_timeout_tester_dag()


# PostgresHook gets the database to stop the query and raise an error if the query runs
# too long. Not testing for a longer statement_timeout than execution_timeout, because
# that doesn't work reliably.
@pytest.mark.parametrize(
    "statement_timeout, default_timeout",
    [
        pytest.param(None, 1, id="default_timeout_only"),
        pytest.param(1, None, id="statement_timeout_only"),
        pytest.param(1, 30, id="statement_timeout_shorter"),
    ],
)
def test_pgdb_raises_cancel_error(statement_timeout, default_timeout):
    with pytest.raises(QueryCanceled):
        hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID,
            default_statement_timeout=default_timeout,
        )
        hook.run("select pg_sleep(10);", statement_timeout=statement_timeout)


# PostgresHook.get_execution_timeout returns the correct number of seconds.
# Trusting Airflow controls on what the execution_timeout can be.
@pytest.mark.parametrize(
    "task_id, expected_result",
    [
        pytest.param("pg_operator_happy", 2, id="pg_operator_happy"),
        pytest.param("pg_hook_happy", 7_200, id="pg_hook_happy"),
        pytest.param("pg_hook_no_timeout", 0, id="pg_hook_no_timeout"),
    ],
)
def test_PostgresHook_get_execution_timeout_happy_tasks(task_id, expected_result):
    ti = TaskInstance(HAPPY_DAG.get_task(task_id), run_id="test_run")
    actual_result = PostgresHook.get_execution_timeout(ti)
    assert actual_result == expected_result


# PGExecuteQueryOperator passes the correct default timeout to the hook.
@pytest.mark.parametrize(
    "execution_timeout, expected_result",
    [
        pytest.param(timedelta(seconds=2), 2.0, id="2s_timeout"),
        pytest.param(timedelta(hours=2), 7_200, id="2h_timeout"),
        pytest.param(None, None, id="no_timeout"),
    ],
)
def test_operator_passes_correct_timeout(execution_timeout, expected_result):
    operator = PGExecuteQueryOperator(
        task_id="test_task_id",
        conn_id=POSTGRES_CONN_ID,
        sql=TEST_SQL,
        execution_timeout=execution_timeout,
    )
    actual_result = operator.get_db_hook().default_statement_timeout
    assert actual_result == expected_result


# Happy path DAG works without error (Each task is a test case)
def test_happy_paths_dag():
    HAPPY_DAG.test()

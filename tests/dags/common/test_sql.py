import logging
from datetime import datetime, timedelta

import pytest
from common.constants import DAG_DEFAULT_ARGS, POSTGRES_CONN_ID
from common.sql import PGExecuteQueryOperator, PostgresHook
from psycopg2.errors import QueryCanceled

from tests.dags.common.test_resources.dags.test_timeout_pg import (
    TEST_SQL,
    create_pg_timeout_tester_dag,
)


logger = logging.getLogger(__name__)


@pytest.fixture
def happy_dag():
    return create_pg_timeout_tester_dag()


DEFAULT_TIMEOUT = timedelta(hours=1).total_seconds()
default_timeout_msg = (
    f"DAG_DEFAULT_ARGS sets the default task execution timeout to "
    f"{DAG_DEFAULT_ARGS['execution_timeout']}. Testing for {DEFAULT_TIMEOUT} seconds."
)


# PostgresHook works with or without explicit connection id, and with override conn id
# Testing mainly for documentation purposes
# Not using pg.run("select 1;") to test the actual connection, because we don't spin up
# the databases for testing.
def test_PostgresHook_init_defaults():
    pg = PostgresHook()
    assert pg.postgres_conn_id == POSTGRES_CONN_ID
    assert pg.default_statement_timeout == DEFAULT_TIMEOUT, default_timeout_msg


def test_PostgresHook_conn_id_only_defaults():
    pg = PostgresHook("gibberish_connection")
    assert pg.postgres_conn_id == "gibberish_connection"
    assert pg.default_statement_timeout == DEFAULT_TIMEOUT, default_timeout_msg


@pytest.mark.parametrize(
    "conn_id, timeout, expected_results",
    [
        pytest.param(None, None, (POSTGRES_CONN_ID, DEFAULT_TIMEOUT), id="both_None"),
        pytest.param(
            "xyzqwerty", None, ("xyzqwerty", DEFAULT_TIMEOUT), id="override_connection"
        ),
        pytest.param(None, 60, (POSTGRES_CONN_ID, 60), id="override_timeout"),
        pytest.param("xyzqwerty", 60, ("xyzqwerty", 60), id="override_both"),
    ],
)
def test_PostgresHook_init_connection(conn_id, timeout, expected_results):
    (expected_conn_id, expected_timeout) = expected_results
    pg = PostgresHook(conn_id, timeout)
    actual_conn_id = pg.postgres_conn_id
    actual_timeout = pg.default_statement_timeout
    assert actual_conn_id == expected_conn_id
    assert actual_timeout == expected_timeout


# PostgresHook gets the database to stop the query and raise an error if the query runs
# too long.
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
            default_statement_timeout=default_timeout,
        )
        start_time = datetime.now()
        hook.run("select pg_sleep(3600);", statement_timeout=statement_timeout)
        end_time = datetime.now()
        assert (end_time - start_time) < timedelta(seconds=2)


# PostgresHook.get_execution_timeout returns the correct number of seconds.
# Trusting Airflow controls on what the execution_timeout can be.
@pytest.mark.parametrize(
    "task_id, expected_result",
    [
        pytest.param("pg_operator_happy", 2.0, id="pg_operator_happy"),
        pytest.param("pg_hook_happy", 7_200.0, id="pg_hook_happy"),
        pytest.param("pg_hook_no_timeout", DEFAULT_TIMEOUT, id="pg_hook_no_timeout"),
    ],
)
def test_PostgresHook_get_execution_timeout_happy_tasks(
    happy_dag, task_id, expected_result
):
    task = happy_dag.get_task(task_id)
    actual_result = PostgresHook.get_execution_timeout(task)
    assert actual_result == expected_result


# PGExecuteQueryOperator passes the correct default timeout to the hook.
@pytest.mark.parametrize(
    "execution_timeout, expected_result",
    [
        pytest.param(timedelta(seconds=2), 2.0, id="2s_timeout"),
        pytest.param(timedelta(hours=2), 7_200, id="2h_timeout"),
        pytest.param(None, DEFAULT_TIMEOUT, id="no_timeout"),
    ],
)
def test_operator_passes_correct_timeout(execution_timeout, expected_result):
    operator = PGExecuteQueryOperator(
        task_id="test_task_id",
        sql=TEST_SQL,
        execution_timeout=execution_timeout,
    )
    actual_result = operator.get_db_hook().default_statement_timeout
    assert actual_result == expected_result


# Happy path DAG works without error (Each task is a test case)
# This generates a warning about running a dag without an explicit data interval being
# deprecated, but I haven't found a way to get it through with this function.
def test_happy_paths_dag(happy_dag):
    happy_dag.test()

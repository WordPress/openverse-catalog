import logging
from collections import namedtuple
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest
from common.sql_helpers import get_task_app_name, pg_runtime_args


logger = logging.getLogger(__name__)


# Set up a list of dicts with attributes and expected results across combinations of
# three scenarios:
#    - a regular single task vs mapping instance
#    - setting args at the beginning of a task (ti) vs killing jobs at failure (context)
#    - explicit execution timeout or default task timeout
#
# Use namedtuple and dict to mock task instance and context to pass as parameters.
# TO DO?: figure out how to future proof this against changes in airflow
#    - Context to mock: top level keys task_instance_key_str, task_instance, task
#        + within task_instance, need .key.map_index
#        + within task, need .execution_timeout
#    - TaskInstance to mock: dag_id, task_id, start_date, key.map_index
#
# TO DO: If we were going to merge this, then we would need to add tests for
# sql_helpers.pg_terminator

TaskTuple = namedtuple("task", "dag_id, task_id, execution_timeout")
KeyTuple = namedtuple("key", "map_index")
TaskInstanceTuple = namedtuple("TaskInstance", "dag_id, task_id, start_date, key, task")

HAPPY_TASK = TaskTuple("happy", "test_task_id", timedelta(seconds=2))
DEFAULT_TIMEOUT_TASK = TaskTuple("default_timeout", "ti", None)


TEST_CASES = [
    {
        "id": "happy_ti_unmapped",
        "ti": TaskInstanceTuple(
            HAPPY_TASK.dag_id,
            HAPPY_TASK.task_id,
            datetime(2023, 1, 24),
            KeyTuple(-1),
            HAPPY_TASK,
        ),
        "expected_app_name": "happy__test_task_id__20230124",
        "expected_sql_args": "SET application_name TO 'happy__test_task_id__20230124'; SET statement_timeout TO '2.0s';",
    },
    {
        "id": "happy_ti_mapped0",
        "ti": TaskInstanceTuple(
            HAPPY_TASK.dag_id,
            HAPPY_TASK.task_id,
            datetime(2023, 1, 24),
            KeyTuple(0),
            HAPPY_TASK,
        ),
        "expected_app_name": "happy__test_task_id__20230124__map0000",
        "expected_sql_args": "SET application_name TO 'happy__test_task_id__20230124__map0000'; SET statement_timeout TO '2.0s';",
    },
    {
        "id": "happy_context_unmapped",
        "ti": {
            "task_instance": TaskInstanceTuple(
                HAPPY_TASK.dag_id,
                HAPPY_TASK.task_id,
                datetime(2023, 1, 24),
                KeyTuple(-1),
                HAPPY_TASK,
            ),
            "task_instance_key_str": "happy__test_task_id__20230124",
            "task": HAPPY_TASK,
        },
        "expected_app_name": "happy__test_task_id__20230124",
        "expected_sql_args": None,  # not applicable because not called on failure
    },
    {
        "id": "happy_context_mapped",
        "ti": {
            "task_instance": TaskInstanceTuple(
                HAPPY_TASK.dag_id,
                HAPPY_TASK.task_id,
                datetime(2023, 1, 24),
                KeyTuple(2),
                HAPPY_TASK,
            ),
            "task_instance_key_str": "happy__test_task_id__20230124",
            "task": HAPPY_TASK,
        },
        "expected_app_name": "happy__test_task_id__20230124__map0002",
        "expected_sql_args": None,  # not applicable because not called on failure
    },
    {
        "id": "none_timeout_context_unmapped",
        "ti": {
            "task_instance": TaskInstanceTuple(
                DEFAULT_TIMEOUT_TASK.dag_id,
                DEFAULT_TIMEOUT_TASK.task_id,
                datetime(2023, 1, 25),
                KeyTuple(-1),
                DEFAULT_TIMEOUT_TASK,
            ),
            "task_instance_key_str": "default_timeout__ti__20230125",
            "task": DEFAULT_TIMEOUT_TASK,
        },
        "expected_app_name": "default_timeout__ti__20230125",
        "expected_sql_args": None,  # not applicable because not called on failure
    },
    {
        "id": "none_timeout_context_mapped",
        "ti": {
            "task_instance": TaskInstanceTuple(
                DEFAULT_TIMEOUT_TASK.dag_id,
                DEFAULT_TIMEOUT_TASK.task_id,
                datetime(2023, 1, 25),
                KeyTuple(2),
                DEFAULT_TIMEOUT_TASK,
            ),
            "task_instance_key_str": "default_timeout__ti__20230125",
            "task": DEFAULT_TIMEOUT_TASK,
        },
        "expected_app_name": "default_timeout__ti__20230125__map0002",
        "expected_sql_args": None,  # not applicable because not called on failure
    },
    {
        "id": "none_timeout_ti_unmapped",
        "ti": TaskInstanceTuple(
            DEFAULT_TIMEOUT_TASK.dag_id,
            DEFAULT_TIMEOUT_TASK.task_id,
            datetime(2023, 1, 25),
            KeyTuple(-1),
            DEFAULT_TIMEOUT_TASK,
        ),
        "expected_app_name": "default_timeout__ti__20230125",
        "expected_sql_args": "SET application_name TO 'default_timeout__ti__20230125'; SET statement_timeout TO '30s';",
    },
    {
        "id": "none_timeout_ti_mapped",
        "ti": TaskInstanceTuple(
            DEFAULT_TIMEOUT_TASK.dag_id,
            DEFAULT_TIMEOUT_TASK.task_id,
            datetime(2023, 1, 25),
            KeyTuple(1),
            DEFAULT_TIMEOUT_TASK,
        ),
        "expected_app_name": "default_timeout__ti__20230125__map0001",
        "expected_sql_args": "SET application_name TO 'default_timeout__ti__20230125__map0001'; SET statement_timeout TO '30s';",
    },
    {
        "id": "grouped_task",
        "ti": TaskInstanceTuple(
            "test_dag",
            "test_group.test_task",
            datetime(2023, 1, 25),
            KeyTuple(-1),
            TaskTuple("test_dag", "test_group.test_task", timedelta(seconds=2)),
        ),
        "expected_app_name": "test_dag__test_group_test_task__20230125",
        "expected_sql_args": "SET application_name TO 'test_dag__test_group_test_task__20230125'; SET statement_timeout TO '2.0s';",
    },
]


@pytest.mark.parametrize("test_case", [pytest.param(t, id=t["id"]) for t in TEST_CASES])
def test_get_task_app_name(test_case):
    expected = test_case["expected_app_name"]
    if "context" in test_case["id"]:
        actual = get_task_app_name(test_case["ti"])
    else:
        with patch("airflow.models.taskinstance.TaskInstance", test_case["ti"]) as ti:
            actual = get_task_app_name(ti)
    assert actual == expected


@pytest.mark.parametrize(
    "test_case",
    # runtime args are only called in the initial task, not the failure callback that
    # uses context instead of task instance.
    [pytest.param(t, id=t["id"]) for t in TEST_CASES if "context" not in t["id"]],
)
def test_pg_runtime_args(test_case):
    expected = test_case["expected_sql_args"]
    if "context" in test_case["id"]:
        actual = pg_runtime_args(test_case["ti"])
    else:
        with patch("airflow.models.taskinstance.TaskInstance", test_case["ti"]) as ti:
            actual = pg_runtime_args(ti)
    assert actual == expected

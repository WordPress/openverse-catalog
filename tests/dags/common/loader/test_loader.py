from unittest import mock

import pytest
from common.loader import loader
from common.loader.reporting import RecordMetrics

from tests.dags.common.test_resources.dags.test_timeout_pg import (
    create_pg_timeout_tester_dag,
)


@pytest.fixture()
def mock_dag():
    return create_pg_timeout_tester_dag()


@pytest.fixture()
def mock_task(mock_dag):
    return mock_dag.get_task("mock_task")


@pytest.mark.parametrize(
    "load_value, clean_data_value, upsert_value, expected, mock_task",
    [
        (100, (10, 15), 75, RecordMetrics(75, 10, 15, 0), mock_task),
        (100, (0, 15), 75, RecordMetrics(75, 0, 15, 10), mock_task),
        (100, (10, 0), 75, RecordMetrics(75, 10, 0, 15), mock_task),
    ],
)
def test_upsert_data_calculations(
    load_value, clean_data_value, upsert_value, expected, mock_task
):
    with mock.patch("common.loader.loader.sql") as sql_mock:
        sql_mock.clean_intermediate_table_data.return_value = clean_data_value
        sql_mock.upsert_records_to_db_table.return_value = upsert_value

        actual = loader.upsert_data(
            postgres_conn_id=mock.Mock(),
            task=mock_task,
            tsv_version="fake",
            identifier="fake",
            media_type="fake",
            loaded_count=load_value,
            duplicates_count=clean_data_value,
        )
        assert actual == expected

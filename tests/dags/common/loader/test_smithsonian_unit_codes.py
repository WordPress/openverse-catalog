import os
from collections import namedtuple
from unittest.mock import Mock, patch

import pytest
from common.loader import smithsonian_unit_codes as si


@pytest.fixture(autouse=True)
def http_hook_mock() -> mock.MagicMock:
    with mock.patch("common.slack.HttpHook") as HttpHookMock:
        yield HttpHookMock.return_value


def test_alert_new_unit_codes():
    unit_code_set = {"a", "b", "c", "d"}
    sub_prov_dict = {"sub_prov1": {"a", "c"}, "sub_prov2": {"b"}, "sub_prov3": {"e"}}

    assert si.get_new_and_outdated_unit_codes(unit_code_set, sub_prov_dict) == (
        {"d"},
        {"e"},
    )


def test_alert_unit_codes_from_api(postgres_with_test_unit_code_table):
    postgres_conn_id = POSTGRES_CONN_ID
    unit_code_table = SI_UNIT_CODE_TABLE
    response_mock = Mock()
    response_mock.json.return_value = {}

    with patch.object(
        si, "get_new_and_outdated_unit_codes", return_value=({"d"}, {"e"})
    ) as mock_get_unit_codes, patch.object(
        si.requests, "get", return_value=response_mock
    ):
        with pytest.raises(Exception):
            si.alert_unit_codes_from_api(postgres_conn_id, unit_code_table)

    mock_get_unit_codes.assert_called()

    postgres_with_test_unit_code_table.cursor.execute(
        f"SELECT * FROM {unit_code_table};"
    )

    actual_rows = postgres_with_test_unit_code_table.cursor.fetchall()
    postgres_with_test_unit_code_table.connection.commit()

    assert len(actual_rows) == 2
    assert ("d", "add") in actual_rows
    assert ("e", "delete") in actual_rows

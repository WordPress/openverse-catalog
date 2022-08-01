from unittest import mock

import pytest
from airflow.exceptions import AirflowException
from maintenance.check_silenced_alerts.check_silenced_alerts import (
    check_configuration,
    get_dags_with_closed_issues,
    get_issue_info,
)

from tests.factories.github import make_issue


@pytest.mark.parametrize(
    "silenced_dags, dags_to_reenable, should_send_alert",
    (
        # No Dags to reenable, don't alert
        ({}, [], False),
        # One DAG to reenable
        (
            {"dag_a_id": "https://github.com/WordPress/openverse/issues/1"},
            [
                ("dag_a_id", "https://github.com/WordPress/openverse/issues/1"),
            ],
            True,
        ),
        # Multiple DAGs to reenable
        (
            {
                "dag_a_id": "https://github.com/WordPress/openverse/issues/1",
                "dag_b_id": "https://github.com/WordPress/openverse/issues/2",
            },
            [
                ("dag_a_id", "https://github.com/WordPress/openverse/issues/1"),
                ("dag_b_id", "https://github.com/WordPress/openverse/issues/2"),
            ],
            True,
        ),
    ),
)
def test_check_configuration(silenced_dags, dags_to_reenable, should_send_alert):
    with mock.patch(
        "maintenance.check_silenced_alerts.check_silenced_alerts.Variable",
        return_value=silenced_dags,
    ), mock.patch(
        "maintenance.check_silenced_alerts.check_silenced_alerts.get_dags_with_closed_issues",
        return_value=dags_to_reenable,
    ) as get_dags_with_closed_issues_mock, mock.patch(
        "maintenance.check_silenced_alerts.check_silenced_alerts.send_alert"
    ) as send_alert_mock:
        message = check_configuration("not_set")
        assert send_alert_mock.called == should_send_alert
        assert get_dags_with_closed_issues_mock.called_with("not_set", silenced_dags)

        # Called with correct dag_ids
        for dag_id, issue_url in dags_to_reenable:
            assert f"<{issue_url}|{dag_id}>" in message


@pytest.mark.parametrize(
    "open_issues, closed_issues",
    (
        # No issues
        ([], []),
        # Only open issues
        (
            [
                "https://github.com/WordPress/openverse/issues/1",
            ],
            [],
        ),
        # Some closed issues, some open
        (
            [
                "https://github.com/WordPress/openverse/issues/1",
            ],
            [
                "https://github.com/WordPress/openverse/issues/2",
            ],
        ),
        # Multiple closed issues
        (
            [],
            [
                "https://github.com/WordPress/openverse/issues/1",
                "https://github.com/WordPress/openverse/issues/2",
            ],
        ),
    ),
)
def test_get_dags_with_closed_issues(open_issues, closed_issues):
    # Mock get_issue
    def mock_get_issue(repo, issue_number, owner):
        url = f"https://github.com/WordPress/openverse/issues/{issue_number}"
        if url in open_issues:
            return make_issue("open")
        return make_issue("closed")

    with mock.patch(
        "maintenance.check_silenced_alerts.check_silenced_alerts.GitHubAPI.get_issue",
    ) as MockGetIssue:
        MockGetIssue.side_effect = mock_get_issue

        silenced_dags = {f"dag_{issue}": issue for issue in open_issues + closed_issues}

        dags_to_reenable = get_dags_with_closed_issues("not_set", silenced_dags)

        assert len(dags_to_reenable) == len(closed_issues)
        for issue in closed_issues:
            assert (f"dag_{issue}", issue) in dags_to_reenable


@pytest.mark.parametrize(
    "url, expected_result",
    (
        (
            "https://github.com/WordPress/openverse/issues/10",
            ("WordPress", "openverse", "10"),
        ),
        (
            "WordPress/openverse-catalog/issues/100",
            ("WordPress", "openverse-catalog", "100"),
        ),
        pytest.param(
            "openverse/issues/10",
            None,
            marks=pytest.mark.raises(
                exception=AirflowException,
                match="Issue url openverse/issues/10 could not be parsed.",
            ),
        ),
    ),
)
def test_get_issue_info(url, expected_result):
    result = get_issue_info(url)
    assert result == expected_result

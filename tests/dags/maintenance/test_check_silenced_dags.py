from unittest import mock

import pytest
from airflow.exceptions import AirflowException, AirflowSkipException
from maintenance.check_silenced_dags.check_silenced_dags import (
    check_configuration,
    get_dags_with_closed_issues,
    get_issue_info,
)

from tests.factories.github import make_issue


@pytest.mark.parametrize(
    "silenced_dags, dags_to_reenable, should_send_alert",
    (
        # No Dags to reenable, task should skip
        pytest.param(
            {},
            [],
            False,
            marks=pytest.mark.raises(exception=AirflowSkipException),
        ),
        # One DAG to reenable
        (
            {
                "dag_a_id": [
                    {
                        "issue": "https://github.com/WordPress/openverse/issues/1",
                        "predicate": "Test exception",
                    }
                ]
            },
            [
                (
                    "dag_a_id",
                    "https://github.com/WordPress/openverse/issues/1",
                    "Test exception",
                ),
            ],
            True,
        ),
        # One DAG, multiple notifications to reenable
        (
            {
                "dag_a_id": [
                    {
                        "issue": "https://github.com/WordPress/openverse/issues/1",
                        "predicate": "Test exception",
                    },
                    {
                        "issue": "https://github.com/WordPress/openverse/issues/1",
                        "predicate": "A different error",
                    },
                ]
            },
            [
                (
                    "dag_a_id",
                    "https://github.com/WordPress/openverse/issues/1",
                    "Test exception",
                ),
                (
                    "dag_a_id",
                    "https://github.com/WordPress/openverse/issues/2",
                    "A different error",
                ),
            ],
            True,
        ),
        # Multiple DAGs to reenable
        (
            {
                "dag_a_id": [
                    {
                        "issue": "https://github.com/WordPress/openverse/issues/1",
                        "predicate": "Test exception",
                    }
                ],
                "dag_b_id": [
                    {
                        "issue": "https://github.com/WordPress/openverse/issues/2",
                        "predicate": "A different error",
                    }
                ],
            },
            [
                (
                    "dag_a_id",
                    "https://github.com/WordPress/openverse/issues/1",
                    "Test exception",
                ),
                (
                    "dag_b_id",
                    "https://github.com/WordPress/openverse/issues/2",
                    "A different error",
                ),
            ],
            True,
        ),
    ),
)
def test_check_configuration(silenced_dags, dags_to_reenable, should_send_alert):
    with (
        mock.patch(
            "maintenance.check_silenced_dags.check_silenced_dags.Variable",
            return_value=silenced_dags,
        ),
        mock.patch(
            "maintenance.check_silenced_dags.check_silenced_dags.get_dags_with_closed_issues",
            return_value=dags_to_reenable,
        ) as get_dags_with_closed_issues_mock,
        mock.patch(
            "maintenance.check_silenced_dags.check_silenced_dags.send_alert"
        ) as send_alert_mock,
    ):
        message = check_configuration("not_set")
        assert send_alert_mock.called == should_send_alert
        assert get_dags_with_closed_issues_mock.called_with("not_set", silenced_dags)

        # Called with correct dag_ids
        for dag_id, issue_url, predicate in dags_to_reenable:
            assert f"<{issue_url}|{dag_id}: {predicate}>" in message


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
        "maintenance.check_silenced_dags.check_silenced_dags.GitHubAPI.get_issue",
    ) as MockGetIssue:
        MockGetIssue.side_effect = mock_get_issue

        silenced_dags = {
            f"dag_{issue}": [{"issue": issue, "predicate": "test"}]
            for issue in open_issues + closed_issues
        }

        dags_to_reenable = get_dags_with_closed_issues("not_set", silenced_dags)

        assert len(dags_to_reenable) == len(closed_issues)
        for issue in closed_issues:
            assert (f"dag_{issue}", issue, "test") in dags_to_reenable


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

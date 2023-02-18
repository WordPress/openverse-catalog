from datetime import timedelta
from unittest import mock

import pytest
from providers.provider_workflows import ProviderWorkflow

from tests.dags.providers.provider_api_scripts.resources.provider_data_ingester.mock_provider_data_ingester import (
    MockAudioOnlyProviderDataIngester,
    MockImageOnlyProviderDataIngester,
    MockProviderDataIngester,
)


@pytest.mark.parametrize(
    "provider_workflow, expected_dag_id",
    [
        # If the ProviderWorkflow defines dag_id, this should be used
        (
            ProviderWorkflow(
                ingester_class=MockProviderDataIngester, dag_id="my_dag_id_override"
            ),
            "my_dag_id_override",
        ),
        # If no dag_id is defined, it should build a default using the module
        # name of the ingester class
        (
            ProviderWorkflow(ingester_class=MockProviderDataIngester),
            "mock_provider_data_ingester_workflow",
        ),
    ],
)
def test_dag_id(provider_workflow, expected_dag_id):
    assert provider_workflow.dag_id == expected_dag_id


@pytest.mark.parametrize(
    "ingester_class, expected_media_types",
    [
        (
            MockAudioOnlyProviderDataIngester,
            [
                "audio",
            ],
        ),
        (
            MockImageOnlyProviderDataIngester,
            [
                "image",
            ],
        ),
        (MockProviderDataIngester, ["audio", "image"]),
    ],
)
def test_sets_media_types(ingester_class, expected_media_types):
    provider_workflow = ProviderWorkflow(ingester_class=ingester_class)

    assert provider_workflow.media_types == expected_media_types


@pytest.mark.parametrize(
    "configuration_overrides, expected_pull, expected_upsert",
    [
        # No overrides configured
        ({}, timedelta(days=1), timedelta(hours=1)),
        # Overrides configured, but not for this dag_id
        (
            {"some_other_dag_id": {"pull_timeout": "00:05:00:00"}},
            timedelta(days=1),
            timedelta(hours=1),
        ),
        # Configured override for pull_timeout only
        (
            {"my_dag_id": {"pull_timeout": "01:12:30:30"}},
            timedelta(days=1, hours=12, minutes=30, seconds=30),
            timedelta(hours=1),
        ),
        # Override for upsert_timeout only
        (
            {"my_dag_id": {"upsert_timeout": "02:6:10:15"}},
            timedelta(days=1),
            timedelta(days=2, hours=6, minutes=10, seconds=15),
        ),
        # Both timeouts overridden
        (
            {
                "my_dag_id": {
                    "pull_timeout": "01:12:30:30",
                    "upsert_timeout": "02:6:10:15",
                }
            },
            timedelta(days=1, hours=12, minutes=30, seconds=30),
            timedelta(days=2, hours=6, minutes=10, seconds=15),
        ),
    ],
)
def test_overrides(configuration_overrides, expected_pull, expected_upsert):
    with mock.patch("providers.provider_workflows.Variable") as MockVariable:
        MockVariable.get.side_effect = [
            configuration_overrides,
        ]
        test_workflow = ProviderWorkflow(
            dag_id="my_dag_id",
            ingester_class=MockProviderDataIngester,
            pull_timeout=timedelta(days=1),
            upsert_timeout=timedelta(hours=1),
        )

        assert test_workflow.pull_timeout == expected_pull
        assert test_workflow.upsert_timeout == expected_upsert


@pytest.mark.parametrize(
    "time_str, expected_timedelta",
    [
        ("0:0:0:10", timedelta(seconds=10)),
        ("30:10:57:45", timedelta(days=30, hours=10, minutes=57, seconds=45)),
        ("0:6:0:0", timedelta(hours=6)),
        ("0:36:0:0", timedelta(days=1, hours=12)),
        # Incorrectly formatted strings returns None
        ("0:1:2", None),
        ("one:2:3:4", None),
        ("foo", None),
        (None, None),
    ],
)
def test_get_timedelta(time_str, expected_timedelta):
    actual_timedelta = ProviderWorkflow._get_timedelta(time_str)
    assert actual_timedelta == expected_timedelta

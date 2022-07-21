from unittest import mock

import pytest
import requests
from airflow.models import TaskInstance
from providers import factory_utils

from tests.dags.common.test_resources import fake_provider_module


@pytest.fixture
def ti_mock() -> TaskInstance:
    return mock.MagicMock(spec=TaskInstance)


@pytest.fixture
def internal_func_mock():
    """
    This mock, along with the value, get handed into the provided function.
    For fake_provider_module.main, the mock will be called with the provided value.
    """
    return mock.MagicMock()


@pytest.mark.parametrize(
    "func, media_types, stores",
    [
        # Happy path
        (fake_provider_module.main, ["image"], [fake_provider_module.image_store]),
        # Empty case, no media types provided
        (fake_provider_module.main, [], []),
        # Provided function doesn't have a store at the module level
        pytest.param(
            requests.get,
            ["image"],
            [None],
            marks=pytest.mark.raises(
                exception=ValueError, match="Expected stores in .*? were missing.*"
            ),
        ),
        # Provided function doesn't have all specified stores
        pytest.param(
            fake_provider_module.main,
            ["image", "other"],
            [None, None],
            marks=pytest.mark.raises(
                exception=ValueError, match="Expected stores in .*? were missing.*"
            ),
        ),
    ],
)
def test_load_provider_script(func, media_types, stores):
    actual_stores = factory_utils._load_provider_script(
        func,
        media_types,
    )
    assert actual_stores == dict(zip(media_types, stores))


@pytest.mark.parametrize(
    "func, media_types, stores",
    [
        # Happy path
        (fake_provider_module.main, ["image"], [fake_provider_module.image_store]),
        # Multiple types
        (
            fake_provider_module.main,
            ["image", "audio"],
            [fake_provider_module.image_store, fake_provider_module.audio_store],
        ),
        # Empty case, no media types provided
        (fake_provider_module.main, [], []),
    ],
)
def test_generate_tsv_filenames(func, media_types, stores, ti_mock, internal_func_mock):
    value = 42
    factory_utils.generate_tsv_filenames(
        func,
        media_types,
        ti_mock,
        args=[internal_func_mock, value],
    )
    # There should be one call to xcom_push for each provided store
    expected_xcoms = len(media_types)
    actual_xcoms = ti_mock.xcom_push.call_count
    assert (
        actual_xcoms == expected_xcoms
    ), f"Expected {expected_xcoms} XComs but {actual_xcoms} pushed"
    for args, store in zip(ti_mock.xcom_push.mock_calls[:-1], stores):
        assert args.kwargs["value"] == store.output_path

    # Check that the function itself was NOT called with the provided args
    internal_func_mock.assert_not_called()


@pytest.mark.parametrize(
    "func, media_types, tsv_filenames, stores",
    [
        # Happy path
        (
            fake_provider_module.main,
            ["image"],
            ["image_file_000.tsv"],
            [fake_provider_module.image_store],
        ),
        # Multiple types
        (
            fake_provider_module.main,
            ["image", "audio"],
            ["image_file_000.tsv", "audio_file_111.tsv"],
            [fake_provider_module.image_store, fake_provider_module.audio_store],
        ),
        # Empty case, no media types provided
        (fake_provider_module.main, [], [], []),
        # Fails if a mismatched # of items is provided
        pytest.param(
            fake_provider_module.main,
            ["image", "other"],
            ["file1.tsv"],
            [fake_provider_module.image_store, fake_provider_module.audio_store],
            marks=pytest.mark.raises(
                exception=ValueError,
                match="Provided media types and TSV filenames don't match.*",
            ),
        ),
        pytest.param(
            fake_provider_module.main,
            ["image"],
            ["file1.tsv", "file2.tsv"],
            [fake_provider_module.image_store],
            marks=pytest.mark.raises(
                exception=ValueError,
                match="Provided media types and TSV filenames don't match.*",
            ),
        ),
    ],
)
def test_pull_media_wrapper(
    func, media_types, tsv_filenames, stores, ti_mock, internal_func_mock
):
    value = 42
    factory_utils.pull_media_wrapper(
        func,
        media_types,
        tsv_filenames,
        ti_mock,
        args=[internal_func_mock, value],
    )
    # We should have one XCom push for duration
    assert ti_mock.xcom_push.call_count == 1
    # Check that the duration was reported
    assert ti_mock.xcom_push.mock_calls[0].kwargs["key"] == "duration"
    # Check that the output paths for the stores were changed to the provided filenames
    for filename, store in zip(tsv_filenames, stores):
        assert store.output_path == filename

    # Check that the function itself was called with the provided args
    internal_func_mock.assert_called_once_with(value)

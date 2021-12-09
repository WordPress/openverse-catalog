from unittest.mock import MagicMock, patch

import pytest
import requests
from common import requester
from requests_oauthlib import OAuth2Session

from tests.dags.conftest import FAKE_OAUTH_PROVIDER_NAME


@patch("common.requester.time")
@pytest.mark.parametrize(
    "delay, last_request, time_value, expected_wait",
    [
        # No wait
        (0, 0, 1, -1),
        (0.2, 0, 1, -1),
        # Some wait
        (10, 0, 2, 8),
        (100.01, 0, 0.01, 100.0),
    ],
)
def test_delay_processing(mock_time, delay, last_request, time_value, expected_wait):
    mock_time.time.return_value = time_value
    dq = requester.DelayedRequester(delay)
    dq._last_request = last_request
    dq._delay_processing()
    if expected_wait >= 0:
        mock_time.sleep.assert_called_with(expected_wait)
    else:
        mock_time.sleep.assert_not_called()


def test_get_handles_exception(monkeypatch):
    def mock_requests_get(url, params, **kwargs):
        raise requests.exceptions.ReadTimeout("test timeout!")

    monkeypatch.setattr(requester.requests, "get", mock_requests_get)

    dq = requester.DelayedRequester(1)
    dq.get("https://google.com/")


def test_get_response_json_retries_with_none_response():
    dq = requester.DelayedRequester(1)
    with patch.object(dq, "get", return_value=None) as mock_get:
        with pytest.raises(Exception):
            assert dq.get_response_json(
                "https://google.com/",
                retries=2,
            )

    assert mock_get.call_count == 3


def test_get_response_json_retries_with_non_ok():
    dq = requester.DelayedRequester(1)
    r = requests.Response()
    r.status_code = 504
    r.json = MagicMock(return_value={"batchcomplete": ""})
    with patch.object(dq, "get", return_value=r) as mock_get:
        with pytest.raises(Exception):
            assert dq.get_response_json(
                "https://google.com/",
                retries=2,
            )

    assert mock_get.call_count == 3


def test_get_response_json_retries_with_error_json():
    dq = requester.DelayedRequester(1)
    r = requests.Response()
    r.status_code = 200
    r.json = MagicMock(return_value={"error": ""})
    with patch.object(dq, "get", return_value=r) as mock_get:
        with pytest.raises(Exception):
            assert dq.get_response_json(
                "https://google.com/",
                retries=2,
            )

    assert mock_get.call_count == 3


def test_get_response_json_returns_response_json_when_all_ok():
    dq = requester.DelayedRequester(1)
    expect_response_json = {"batchcomplete": ""}
    r = requests.Response()
    r.status_code = 200
    r.json = MagicMock(return_value=expect_response_json)
    with patch.object(dq, "get", return_value=r) as mock_get:
        actual_response_json = dq.get_response_json(
            "https://google.com/",
            retries=2,
        )

    assert mock_get.call_count == 1
    assert actual_response_json == expect_response_json


def test_oauth_requester_initializes_correctly(oauth_provider_var_mock):
    odq = requester.OAuth2DelayedRequester(FAKE_OAUTH_PROVIDER_NAME, 1)
    assert isinstance(odq.session, OAuth2Session)
    assert odq.session.client_id == "fakeclient"

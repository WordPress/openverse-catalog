import logging
import time
from unittest.mock import MagicMock, patch

import pytest
import requests
from common import requester
from requests_oauthlib import OAuth2Session

from tests.dags.conftest import FAKE_OAUTH_PROVIDER_NAME


def test_get_waits_before_getting(monkeypatch):
    delay = 0.2

    def mock_requests_get(url, params, **kwargs):
        return requests.Response()

    monkeypatch.setattr(requester.requests.Session, "get", mock_requests_get)
    dq = requester.DelayedRequester(delay)
    s = time.time()
    dq.get("https://google.com")
    print(time.time() - s)
    start = time.time()
    dq.get("https://google.com")
    assert time.time() - start >= delay


def test_get_handles_exception(monkeypatch, caplog):
    def mock_requests_get(url, params, **kwargs):
        raise requests.exceptions.ReadTimeout("test timeout!")

    dq = requester.DelayedRequester(1)
    monkeypatch.setattr(dq.session, "get", mock_requests_get)

    with caplog.at_level(logging.WARNING):
        dq.get("https://google.com/")
        assert "Error with the request for URL: https://google.com/" in caplog.text


@pytest.mark.parametrize(
    "code, log_level, expected_message",
    [
        (500, logging.WARNING, "Unable to request URL"),
        (401, logging.ERROR, "Authorization failed for URL"),
    ],
)
def test_get_handles_failure_status_codes(
    code, log_level, expected_message, monkeypatch, caplog
):
    url = "https://google.com/"
    mock_response = MagicMock()
    mock_response.status_code = code
    mock_response.url = url

    def mock_requests_get(url, params, **kwargs):
        return mock_response

    dq = requester.DelayedRequester(1)
    monkeypatch.setattr(dq.session, "get", mock_requests_get)

    with caplog.at_level(log_level):
        dq.get(url)
        assert f"{expected_message}: {url}" in caplog.text


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

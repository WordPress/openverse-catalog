import logging
import time
from unittest.mock import MagicMock, patch

import pytest
import requests
from requests_oauthlib import OAuth2Session

from catalog.tests.dags.conftest import FAKE_OAUTH_PROVIDER_NAME
from common import requester


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


def test_get_delays_processing(monkeypatch):
    def mock_requests_get(url, params, **kwargs):
        r = requests.Response()
        r.status_code = 200
        return r

    monkeypatch.setattr(requester.requests.Session, "get", mock_requests_get)

    delay = 1
    dq = requester.DelayedRequester(delay=delay)
    start = time.time()
    dq.get("http://fake_url")
    dq.get("http://fake_url")
    end = time.time()
    assert end - start >= delay


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


@pytest.mark.parametrize(
    "init_headers, request_kwargs, expected_request_kwargs",
    [
        (None, None, {"headers": {}}),
        ({"init_header": "test"}, None, {"headers": {"init_header": "test"}}),
        (
            None,
            {"headers": {"h1": "test1"}, "other_kwarg": "test"},
            {"headers": {"h1": "test1"}, "other_kwarg": "test"},
        ),
        (None, {"headers": {"h2": "test2"}}, {"headers": {"h2": "test2"}}),
        (
            {"init_header": "test"},
            {"headers": {"h2": "test2"}},
            {"headers": {"h2": "test2"}},
        ),
        ({"init_header": "test"}, {"headers": None}, {"headers": None}),
        ({"init_header": "test"}, {"headers": {}}, {"headers": {}}),
    ],
    ids=[
        "none_none",
        "default_only",
        "req_only_other",
        "req_only_no_other",
        "both",
        "override_to_none",
        "override_to_empty",
    ],
)
def test_handles_optional_headers(
    init_headers, request_kwargs, expected_request_kwargs
):
    dq = requester.DelayedRequester(0, headers=init_headers)
    dq.session.get = MagicMock(return_value=None)
    url = "http://test"
    params = {"testy": "test"}
    dq.get(url, params, **(request_kwargs or {}))
    dq.session.get.assert_called_once_with(
        url, params=params, **(expected_request_kwargs or {})
    )

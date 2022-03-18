from unittest import mock

import pytest
from common.loader.reporting import report_completion


@pytest.fixture(autouse=True)
def send_message_mock() -> mock.MagicMock:
    with mock.patch("common.slack.SlackMessage.send") as SendMessageMock:
        yield SendMessageMock


@pytest.mark.parametrize(
    "should_send_message",
    [True, False],
)
def test_report_completion(should_send_message):
    with mock.patch(
        "common.slack.should_send_message", return_value=should_send_message
    ):
        report_completion("Jamendo", "Audio", None, 100)
        # Send message is only called if `should_send_message` is True.
        send_message_mock.called = should_send_message


@pytest.mark.parametrize(
    "duration, expected",
    [
        (None, "_No data_"),
        (100.5, "100.5"),
        (1234567.9999, "1234568.0"),  # Gets rounded up
        (0.123456, "0.12"),
    ],
)
def test_duration_truncated(duration, expected):
    with mock.patch("common.loader.reporting.send_message") as send_message:
        report_completion("Jamendo", "Audio", duration, 100)
        message = send_message.call_args.args[0]
        assert expected in message

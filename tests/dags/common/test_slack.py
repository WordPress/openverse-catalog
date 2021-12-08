from datetime import datetime
from unittest import mock

import pytest
from airflow.exceptions import AirflowNotFoundException
from common.slack import SlackMessage, on_failure_callback, send_message


_FAKE_IMAGE = "http://image.com/img.jpg"


@pytest.fixture(autouse=True)
def http_hook_mock() -> mock.MagicMock:
    with mock.patch("common.slack.HttpHook") as HttpHookMock:
        yield HttpHookMock.return_value


@pytest.mark.parametrize(
    "plain_text, expected",
    [
        (
            False,
            {"type": "mrkdwn", "text": "test message"},
        ),
        (
            True,
            {"type": "plain_text", "text": "test message"},
        ),
    ],
)
def test_text_section(plain_text, expected):
    actual = SlackMessage._text_block("test message", plain_text)
    assert actual == expected


@pytest.mark.parametrize(
    "url, title, alt_text, expected",
    [
        (
            _FAKE_IMAGE,
            None,
            None,
            {"type": "image", "image_url": _FAKE_IMAGE, "alt_text": "img.jpg"},
        ),
        (
            _FAKE_IMAGE,
            "Sample title",
            None,
            {
                "type": "image",
                "image_url": _FAKE_IMAGE,
                "title": {"type": "plain_text", "text": "Sample title"},
                "alt_text": "img.jpg",
            },
        ),
        (
            _FAKE_IMAGE,
            None,
            "Sample alternative text",
            {
                "type": "image",
                "image_url": _FAKE_IMAGE,
                "alt_text": "Sample alternative text",
            },
        ),
        (
            _FAKE_IMAGE,
            "Both title",
            "And alt text",
            {
                "type": "image",
                "image_url": _FAKE_IMAGE,
                "title": {"type": "plain_text", "text": "Both title"},
                "alt_text": "And alt text",
            },
        ),
    ],
)
def test_image_section(url, title, alt_text, expected):
    actual = SlackMessage._image_block(url, title, alt_text)
    assert actual == expected


def test_clear():
    s = SlackMessage()
    s.blocks = [{"text": "fake"}, {"text": "fake2"}]
    s._context = {"fake-context": "value"}
    s._payload = {"fake-payload": "value"}
    s.clear()
    assert s.blocks == []
    assert s._context == {}
    assert s._payload == s._base_payload


def test_payload_property():
    s = SlackMessage()
    s.blocks = [{"text": "fake"}, {"text": "fake2"}]
    assert s.payload == {
        "blocks": [{"text": "fake"}, {"text": "fake2"}],
        "icon_emoji": ":airflow:",
        "unfurl_links": True,
        "unfurl_media": True,
        "username": "Airflow",
    }


def test_add_context_no_initial_context():
    s = SlackMessage()
    assert s._context == {}
    s.add_context("Sample context")
    assert s._context == {
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": "Sample context"}],
    }


def test_add_context_multiple():
    s = SlackMessage()
    s.add_context("Sample context")
    s.add_context("Additional context")
    assert s._context == {
        "type": "context",
        "elements": [
            {"type": "mrkdwn", "text": "Sample context"},
            {"type": "mrkdwn", "text": "Additional context"},
        ],
    }


def test_add_context_too_many():
    s = SlackMessage()
    with pytest.raises(
        ValueError, match="Unable to include more than 10 context elements"
    ):
        for idx in range(20):
            s.add_context(f"Sample context {idx}")


def test_add_context_image_no_initial_context():
    s = SlackMessage()
    assert s._context == {}
    s.add_context_image(_FAKE_IMAGE, alt_text="fake alt")
    assert s._context == {
        "type": "context",
        "elements": [
            {
                "alt_text": "fake alt",
                "image_url": "http://image.com/img.jpg",
                "type": "image",
            }
        ],
    }


def test_add_context_image_multiple():
    s = SlackMessage()
    s.add_context_image(_FAKE_IMAGE, alt_text="fake alt")
    s.add_context_image(_FAKE_IMAGE, alt_text="other alt")
    assert s._context == {
        "type": "context",
        "elements": [
            {
                "alt_text": "fake alt",
                "image_url": "http://image.com/img.jpg",
                "type": "image",
            },
            {
                "alt_text": "other alt",
                "image_url": "http://image.com/img.jpg",
                "type": "image",
            },
        ],
    }


def test_add_context_image_too_many():
    s = SlackMessage()
    with pytest.raises(
        ValueError, match="Unable to include more than 10 context elements"
    ):
        for idx in range(20):
            s.add_context_image(_FAKE_IMAGE, alt_text=f"Alt: {idx}")


def test_add_block():
    s = SlackMessage()
    s._add_block({"fake": "value"})
    assert s._context == {}
    assert s.blocks == [{"fake": "value"}]


def test_add_block_with_context():
    s = SlackMessage()
    s.add_context("Additional context")
    s._add_block({"fake": "value"})
    assert s._context == {}
    assert s.blocks == [
        {
            "type": "context",
            "elements": [{"type": "mrkdwn", "text": "Additional context"}],
        },
        {"fake": "value"},
    ]


def test_add_divider():
    s = SlackMessage()
    s.add_divider()
    assert s.blocks == [{"type": "divider"}]


def test_add_text():
    s = SlackMessage()
    s.add_text("Fake message")
    assert s.blocks == [
        {"type": "section", "text": {"type": "mrkdwn", "text": "Fake message"}}
    ]


def test_add_image():
    s = SlackMessage()
    s.add_image(_FAKE_IMAGE)
    assert s.blocks == [
        {
            "type": "image",
            "image_url": "http://image.com/img.jpg",
            "alt_text": "img.jpg",
        }
    ]


def test_send_no_message():
    s = SlackMessage()
    with pytest.raises(ValueError, match="Nothing to send!"):
        s.send()


def test_send_no_context(http_hook_mock):
    s = SlackMessage()
    s.blocks = [1, 2, 3]
    s.send()
    http_hook_mock.run.assert_called_with(
        endpoint=None,
        data='{"username": "Airflow", "unfurl_links": true, "unfurl_media": true, '
        '"icon_emoji": ":airflow:", "blocks": [1, 2, 3], '
        '"text": "Airflow notification"}',
        headers={"Content-type": "application/json"},
        extra_options={"verify": True},
    )
    assert s._payload == s._base_payload


def test_send_with_context(http_hook_mock):
    s = SlackMessage()
    s.blocks = [1, 2, 3]
    s.add_context("Sample context")
    s.send()
    http_hook_mock.run.assert_called_with(
        endpoint=None,
        data='{"username": "Airflow", "unfurl_links": true, "unfurl_media": true, '
        '"icon_emoji": ":airflow:", "blocks": [1, 2, 3, {"type": "context", '
        '"elements": [{"type": "mrkdwn", "text": "Sample context"}]}], '
        '"text": "Airflow notification"}',
        headers={"Content-type": "application/json"},
        extra_options={"verify": True},
    )
    assert s._payload == s._base_payload


def test_send_fails(http_hook_mock):
    s = SlackMessage()
    # Cause an exception within the raise_for_status call
    http_hook_mock.run.return_value.raise_for_status.side_effect = Exception(
        "HTTP Error 666"
    )
    s.add_text("Sample message")
    with pytest.raises(Exception, match="HTTP Error 666"):
        s.send()


def test_send_message(http_hook_mock):
    send_message("Sample text", username="DifferentUser")
    http_hook_mock.run.assert_called_with(
        endpoint=None,
        data='{"username": "DifferentUser", "unfurl_links": true, "unfurl_media": true,'
        ' "icon_emoji": ":airflow:", "blocks": [{"type": "section", "text": '
        '{"type": "mrkdwn", "text": "Sample text"}}], "text": "Sample text"}',
        headers={"Content-type": "application/json"},
        extra_options={"verify": True},
    )


@pytest.mark.parametrize(
    "exception, environment, force_slack_alert, call_expected",
    [
        # Message with exception
        (ValueError("Whoops!"), "dev", False, False),
        (ValueError("Whoops!"), "dev", True, True),
        (ValueError("Whoops!"), "prod", False, True),
        (ValueError("Whoops!"), "prod", True, True),
        # Message without exception
        (None, "dev", False, False),
        (None, "dev", True, True),
        (None, "prod", False, True),
        (None, "prod", True, True),
        # Exception with upstream failure message should never run
        (ValueError("Upstream task(s) failed"), "dev", False, False),
        (ValueError("Upstream task(s) failed"), "dev", True, False),
        (ValueError("Upstream task(s) failed"), "prod", False, False),
        (ValueError("Upstream task(s) failed"), "prod", True, False),
    ],
)
def test_on_failure_callback(
    exception, environment, force_slack_alert, call_expected, http_hook_mock
):
    context = {
        "task_instance": mock.Mock(),
        "execution_date": datetime.now(),
        "exception": exception,
    }
    with mock.patch("common.slack.Variable") as MockVariable:
        run_mock = http_hook_mock.run
        # Mock the calls to Variable.get, in order
        MockVariable.get.side_effect = [environment, force_slack_alert]
        on_failure_callback(context)
        assert run_mock.called == call_expected
        if call_expected:
            # Check that an exception message is present only if one is provided
            assert bool(exception) ^ (
                "Exception" not in run_mock.call_args.kwargs["data"]
            )


def test_on_failure_callback_does_nothing_without_hook(http_hook_mock):
    http_hook_mock.get_conn.side_effect = AirflowNotFoundException("nope")
    on_failure_callback({})
    http_hook_mock.run.assert_not_called()

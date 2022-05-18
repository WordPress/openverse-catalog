from unittest import mock

import pytest
from database.report_pending_reported_images import report_actionable_records


@pytest.mark.parametrize(
    "distinct_image_reports, expected_message",
    [
        (0, "No images require review at this time"),
        (10, "10 reported images pending review"),
    ],
)
def test_reports_reported_images(distinct_image_reports, expected_message):
    with mock.patch("common.slack.send_message") as send_message_mock:
        report_actionable_records(distinct_image_reports)
        assert (
            expected_message in send_message_mock.call_args.args[0]
        ), "Completion message doesn't contain expected text"

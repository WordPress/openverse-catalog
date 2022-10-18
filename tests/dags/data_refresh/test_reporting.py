import pytest
from data_refresh.reporting import report_record_difference, report_status


@pytest.mark.parametrize(
    "before, after, expected_in_message",
    [
        ["1", "2", ["1 → 2", "+1 (+100.000%"]],
        ["1", "3", ["1 → 3", "+2 (+200.000%"]],
        ["4", "2", ["4 → 2", "-2 (-50.000%"]],
        ["4000", "2000", ["4,000 → 2,000", "-2,000 (-50.000%"]],
        ["174290", "817198", ["174,290 → 817,198", "+642,908 (+368.873%"]],
        ["563561088", "564775616", ["563,561,088 → 564,775,616", "+1,214,528 (+200.000%"]],
        ["579553984", "579554880", ["579,553,984 → 579,554,880", "+896 (+0.000%"]],
    ],
)
def test_record_reporting(before, after, expected_in_message):
    actual = report_record_difference(before, after, "media", "dag_id")
    assert isinstance(expected_in_message, list), (
        "Value for 'expected_in_message' should be a list, "
        "a string may give a false positive"
    )
    for expected in expected_in_message:
        assert expected in actual


def test_report_status():
    actual = report_status("image", "This is my message", "sample_dag_id")
    assert actual == "`image`: This is my message"

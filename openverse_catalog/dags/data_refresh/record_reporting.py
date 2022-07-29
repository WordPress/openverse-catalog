import logging

from common import slack


log = logging.getLogger(__name__)


def report_record_difference(before: str, after: str, media_type: str):
    before = int(before)
    after = int(after)
    count_diff = after - before
    percent_diff = (count_diff / before) * 100
    # Note for formatting:
    # '+' - number will always have a sign in front of it
    # ',' - number is comma separated
    # '.' - number is a float
    message = f"""
**Record count difference for `{media_type}`**: ~{before:,} → ~{after:,}
**Change**: ~{count_diff:+,} (~{percent_diff}% Δ)
"""
    log.info(message)
    if slack.should_send_message():
        slack.send_message(text=message, username="Data refresh record difference")

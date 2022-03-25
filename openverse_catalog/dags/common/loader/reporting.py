from __future__ import annotations

import logging
from typing import Optional

from common.slack import send_message


logger = logging.getLogger(__name__)


# Shamelessly lifted from:
# https://gist.github.com/borgstrom/936ca741e885a1438c374824efb038b3
TIME_DURATION_UNITS = (
    ("week", 60 * 60 * 24 * 7),
    ("day", 60 * 60 * 24),
    ("hour", 60 * 60),
    ("min", 60),
    ("sec", 1),
)
RecordCounts = dict[str, tuple[Optional[int], Optional[int]]]


def humanize_time_duration(seconds: float) -> str:
    if seconds == 0:
        return "inf"
    parts = []
    for unit, div in TIME_DURATION_UNITS:
        amount, seconds = divmod(int(seconds), div)
        if amount > 0:
            parts.append("{} {}{}".format(amount, unit, "" if amount == 1 else "s"))
    return ", ".join(parts)


def report_completion(
    provider_name: str,
    duration: float | str | None,
    record_counts_by_media_type: RecordCounts,
) -> None:
    """
    Send a Slack notification when the load_data task has completed.
    Messages are only sent out in production and if a Slack connection is defined.
    In all cases the data is logged.
    """
    # Truncate the duration value if it's provided
    if isinstance(duration, float):
        duration = humanize_time_duration(duration)

    # List record count per media type
    media_type_reports = ""
    for media_type, (loaded, upserted) in record_counts_by_media_type.items():
        media_type_reports += f"  - `{media_type}`: {upserted or '_No data_'}"
        if upserted is None or loaded is None:
            # Can't make calculation without data
            continue
        duplicates = loaded - upserted
        if duplicates:
            media_type_reports += f" _({duplicates} duplicates)_"
        media_type_reports += "\n"

    # Collect data into a single message
    message = f"""
*Provider*: `{provider_name}`
*Duration of data pull task*: {duration or '_No data_'}
*Number of records upserted per media type*:
{media_type_reports}

* _Duration includes time taken to pull data of all media types._
"""
    send_message(message, username="Airflow DAG Load Data Complete")
    logger.info(message)

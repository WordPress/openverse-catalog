import logging

from common.slack import send_message


logger = logging.getLogger(__name__)
XCOM_PULL_TEMPLATE = "{{{{ ti.xcom_pull(task_ids='{}', key='{}') }}}}"

# Shamelessly lifted from:
# https://gist.github.com/borgstrom/936ca741e885a1438c374824efb038b3
TIME_DURATION_UNITS = (
    ("week", 60 * 60 * 24 * 7),
    ("day", 60 * 60 * 24),
    ("hour", 60 * 60),
    ("min", 60),
    ("sec", 1),
)


def humanize_time_duration(seconds):
    if seconds == 0:
        return "inf"
    parts = []
    for unit, div in TIME_DURATION_UNITS:
        amount, seconds = divmod(int(seconds), div)
        if amount > 0:
            parts.append("{} {}{}".format(amount, unit, "" if amount == 1 else "s"))
    return ", ".join(parts)


def report_completion(provider_name, load_data_stats_by_media_type):
    """
    Send a Slack notification when the load_data task has completed.
    Messages are only sent out in production and if a Slack connection is defined.
    In all cases the data is logged.
    """
    # Truncate the duration value if it's provided
    if isinstance(duration, float):
        duration = humanize_time_duration(duration)

    media_type_reports = []
    for media_type, data in load_data_stats_by_media_type.items():
        # Truncate the duration value if it's provided
        if isinstance(data["duration"], float):
            data["duration"] = round(data["duration"], 2)

        # Build the status report for this media type
        message = f"""
    *Media Type*: `{media_type}`
        *Number of records upserted*: {data['record_count']}
        *Duration of data pull task*: {data['duration']}
        """
        media_type_reports.append(message)

    # Collect data into a single message
    message = (
        f"\n*Provider*: `{provider_name}`\n"
        + "".join(media_type_reports)
        + "\n* _Duration includes time taken to pull data of all media types._"
    )

    send_message(message, username="Airflow DAG Load Data Complete")
    logger.info(message)

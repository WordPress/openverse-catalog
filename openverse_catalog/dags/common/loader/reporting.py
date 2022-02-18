import logging


logger = logging.getLogger(__name__)


def report_completion(provider_name, media_type, duration, record_count):
    message = f"""
*Provider*: `{provider_name}`
*Media Type*: `{media_type}`
*Number of Records Upserted*: {record_count}
*Duration of data pull task**: {duration}

* Duration includes time taken to pull data of all media types.
"""
    logger.info(message)

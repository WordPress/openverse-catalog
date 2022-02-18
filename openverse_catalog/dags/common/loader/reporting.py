import logging

import common


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)
logging.getLogger(common.urls.__name__).setLevel(logging.WARNING)


def report_completion(
    provider_name,
    media_type,
    duration,
):
    logger.info("Load complete")
    logger.info(provider_name)
    logger.info(media_type)
    logger.info(duration)

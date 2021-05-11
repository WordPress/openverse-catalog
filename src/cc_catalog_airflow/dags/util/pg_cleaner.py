"""
This file provides the pieces to perform an after-the-fact processing
of all data in the image table of the upstream DB through the ImageStore
class.
"""
from collections import namedtuple
import logging
import os
from pathlib import Path
from textwrap import dedent
import time

from airflow.providers.postgres.hooks.postgres import PostgresHook

from provider_api_scripts.common.storage import image
from util import tsv_cleaner
from util.loader import column_names as col
from util.loader.sql import IMAGE_TABLE_NAME

logger = logging.getLogger(__name__)
logging.getLogger(image.__name__).setLevel(logging.WARNING)
logging.getLogger(image.columns.urls.__name__).setLevel(logging.WARNING)

MAX_DIR_SIZE = 8 * 1024 ** 3
OUTPUT_DIR = os.path.realpath(os.getenv("OUTPUT_DIR", "/tmp/"))
OVERWRITE_DIR = "overwrite/"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, OVERWRITE_DIR)
DELAY_MINUTES = 1

IMAGE_TABLE_COLS = [
    # These are not precisely the same names as in the DB.
    "identifier",
    "created_on",
    "updated_on",
    "ingestion_type",
    "provider",
    "source",
    "foreign_identifier",
    "foreign_landing_url",
    "image_url",
    "thumbnail_url",
    "width",
    "height",
    "filesize",
    "license_",
    "license_version",
    "creator",
    "creator_url",
    "title",
    "meta_data",
    "tags",
    "watermarked",
    "last_synced",
    "removed",
]

ImageTableRow = namedtuple("ImageTableRow", IMAGE_TABLE_COLS)


class ImageStoreDict(dict):
    def __missing__(self, key):
        ret = self[key] = self._init_image_store(key)
        return ret

    def _init_image_store(
        self,
        key,
        output_path=OUTPUT_PATH,
    ):
        os.makedirs(output_path, exist_ok=True)
        return image.ImageStore(
            provider=key[0],
            output_file=f"cleaned_{key[1]}.tsv",
            output_dir=output_path,
        )


class CleaningException(Exception):
    pass


def clean_prefix_loop(
    postgres_conn_id,
    prefix,
    desired_prefix_length=4,
    delay_minutes=DELAY_MINUTES,
):
    failure = False
    if len(prefix) >= desired_prefix_length:
        try:
            clean_rows(postgres_conn_id, prefix)
        except Exception as e:
            failure = True
            logger.error(f"Failed to clean rows with prefix {prefix}")
            logger.error(f"Exception was {e}")
    else:
        interfix_length = desired_prefix_length - len(prefix)
        for i in hex_counter(interfix_length):
            start_time = time.time()
            try:
                clean_rows(postgres_conn_id, prefix + i)
            except Exception as e:
                failure = True
                logger.error(f"Failed to clean rows with prefix {prefix}")
                logger.error(f"Exception was {e}")
            total_time = time.time() - start_time
            logger.info(f"Total time:  {total_time} seconds")
            _wait_for_space()
    if failure:
        raise CleaningException()


def clean_rows(postgres_conn_id, prefix):
    """
    This function runs all rows from the image table whose identifier
    starts with the given prefix through the ImageStore class, and
    updates them with the result.
    """
    image_store_dict = ImageStoreDict()
    selected_rows = _select_records(postgres_conn_id, prefix)
    total_rows = len(selected_rows)
    logger.info(f"Processing {total_rows} rows from prefix {prefix}.")
    for record in selected_rows:
        try:
            _clean_single_row(record, image_store_dict, prefix)
        except Exception as e:
            logger.warning(f"Record {record} could not be cleaned!")
            logger.warning(f"Error cleaning was: {e}")

    for image_store in image_store_dict.values():
        image_store.commit()

    _log_and_check_totals(total_rows, image_store_dict)


def _wait_for_space(
        min_polling_frequency=5,
        max_polling_frequency=120,
        delay_step=5,
        max_dir_size=MAX_DIR_SIZE,
        output_path=OUTPUT_PATH,
):
    delay = max_polling_frequency
    check_dir = Path(output_path)
    total_wait_time = 0
    logger.info(f"Waiting for space in {output_path}")
    while True:
        du = sum(
            f.stat().st_size for f in check_dir.glob('**/*') if f.is_file()
        )
        if du < max_dir_size:
            break
        else:
            logger.info(
                f"{output_path} holds {du / 1024**2} MB,"
                f" but max is {max_dir_size / 1024**2} MB."
                f" Waiting for {delay} seconds"
            )
            time.sleep(delay)
            total_wait_time += delay
            delay = max(delay - delay_step, min_polling_frequency)
    logger.info(f"Total wait time: {total_wait_time} seconds")


def hex_counter(length):
    max_string = "f" * length
    format_string = f"0{length}x"
    for h in range(int(max_string, 16) + 1):
        yield format(h, format_string)


def _select_records(postgres_conn_id, prefix, image_table=IMAGE_TABLE_NAME):
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    min_base_uuid = "00000000-0000-0000-0000-000000000000"
    max_base_uuid = "ffffffff-ffff-ffff-ffff-ffffffffffff"
    min_uuid = prefix + min_base_uuid[len(prefix):]
    max_uuid = prefix + max_base_uuid[len(prefix):]
    select_query = dedent(
        f"""
        SELECT
          {col.IDENTIFIER}, {col.CREATED_ON}, {col.UPDATED_ON},
          {col.INGESTION_TYPE}, {col.PROVIDER}, {col.SOURCE}, {col.FOREIGN_ID},
          {col.LANDING_URL}, {col.DIRECT_URL}, {col.THUMBNAIL}, {col.WIDTH},
          {col.HEIGHT}, {col.FILESIZE}, {col.LICENSE}, {col.LICENSE_VERSION},
          {col.CREATOR}, {col.CREATOR_URL}, {col.TITLE}, {col.META_DATA},
          {col.TAGS}, {col.WATERMARKED}, {col.LAST_SYNCED}, {col.REMOVED}
        FROM {image_table}
        WHERE
          {col.IDENTIFIER}>='{min_uuid}'::uuid
          AND
          {col.IDENTIFIER}<='{max_uuid}'::uuid;
        """
    )
    return postgres.get_records(select_query)


def _clean_single_row(record, image_store_dict, prefix):
    dirty_row = ImageTableRow(*record)
    image_store = image_store_dict[(dirty_row.provider, prefix)]
    total_images_before = image_store.total_items
    license_lower = dirty_row.license_.lower() if dirty_row.license_ else None
    tags_list = [t for t in dirty_row.tags if t] if dirty_row.tags else None
    image_store.add_item(
        foreign_landing_url=dirty_row.foreign_landing_url,
        image_url=dirty_row.image_url,
        thumbnail_url=dirty_row.thumbnail_url,
        license_url=tsv_cleaner.get_license_url(dirty_row.meta_data),
        license_=license_lower,
        license_version=dirty_row.license_version,
        foreign_identifier=dirty_row.foreign_identifier,
        width=dirty_row.width,
        height=dirty_row.height,
        creator=dirty_row.creator,
        creator_url=dirty_row.creator_url,
        title=dirty_row.title,
        meta_data=dirty_row.meta_data,
        raw_tags=tags_list,
        watermarked=dirty_row.watermarked,
        source=dirty_row.source,
    )
    if not image_store.total_items - total_images_before == 1:
        logger.warning(f"Record {dirty_row} was not stored!")
        _save_failure_identifier(dirty_row.identifier)


def _save_failure_identifier(identifier, output_path=OUTPUT_PATH):
    failure_dir = os.path.join(output_path, "cleaning_failures")
    failure_file = f"fails_{int(time.time()) // 100 * 100}.txt"
    os.makedirs(failure_dir, exist_ok=True)
    failure_full_path = os.path.join(failure_dir, failure_file)
    with open(failure_full_path, "a") as f:
        f.write(f"{identifier}\n")


def _log_and_check_totals(total_rows, image_store_dict):
    image_totals = {k: v.total_items for k, v in image_store_dict.items()}
    total_images_sum = sum(image_totals.values())
    logger.info(f"Total images cleaned:  {total_images_sum}")
    logger.info(f"Image Totals breakdown:  {image_totals}")
    try:
        assert total_images_sum == total_rows
    except Exception as e:
        logger.warning("total_images_sum NOT EQUAL TO total_rows!")
        logger.warning(f"total_images_sum: {total_images_sum}")
        logger.warning(f"total_rows: {total_rows}")
        raise e

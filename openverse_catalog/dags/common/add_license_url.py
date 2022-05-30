import logging
from textwrap import dedent

from airflow.providers.postgres.hooks.postgres import PostgresHook
from common.licenses.constants import get_reverse_license_path_map
from common.slack import send_message
from common.storage import columns as col


RETURN_ROW_COUNT = lambda c: c.rowcount  # noqa: E731

logger = logging.getLogger(__name__)

base_url = "https://creativecommons.org/"

license_map = get_reverse_license_path_map()


def make_sample_data(postgres_conn_id: str):
    logger.info(
        "Dropping license_url column."
        "Replacing 100 items' meta_data fields with null, blank jsonb object, "
        "or an object without `license_url` but with "
        "`some_other_property` for testing purposes."
    )
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)

    postgres.run("""ALTER TABLE image DROP COLUMN IF EXISTS license_url;""")
    result = postgres.get_records(
        dedent(
            """
        SELECT * FROM image WHERE meta_data IS NOT NULL
        LIMIT 100;
        """
        )
    )

    values = {
        0: "null",
        1: "'{}'",
        2: '\'{"some_other_property": "some_other_value"}\'',
    }

    for i, item in enumerate(result):
        image_id = item[0]
        value = values.get(i % 3, "null")
        postgres.run(
            dedent(
                f"""
            UPDATE image SET meta_data = {value} WHERE identifier = '{image_id}'
            """
            )
        )
    logger.info(f"{result} image records added to sample data.")


def get_statistics(postgres_conn_id: str, dag_run):
    logger.info("Getting image records without license_url in meta_data.")
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    no_license_url_count = postgres.get_first(
        dedent(
            """
        SELECT COUNT(*) from image
        WHERE (
        meta_data IS NULL OR (
            meta_data IS NOT NULL AND NOT meta_data ? 'license_url'
            )
        );
        """
        )
    )[0]

    logger.info(f"There are {no_license_url_count} records without license_url.")
    is_test = dag_run.conf.get("isTest")
    if is_test:
        return "make_sample_data"
    elif no_license_url_count > 0:
        return "add_blank_metadata"
    else:
        return "move_columns_to_metadata"


def update_license_url(postgres_conn_id: str):
    """
    Adds license_url to meta_data batching all records with the same license.
    :param postgres_conn_id: Postgres connection id
    :return:
    """

    logger.info("Getting image records without license_url.")
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    postgres.run(
        dedent(
            """
            ALTER TABLE image
            ADD COLUMN IF NOT EXISTS license_url character varying(200);
            """
        )
    )
    total_count = 0
    for license_items, path in license_map.items():
        license_name, license_version = license_items
        logger.info(f"Processing {license_name} {license_version}, {license_items}.")
        license_url = f"{base_url}{path}/"
        update_license_url_query = dedent(
            f"""
            UPDATE image
            SET license_url = '{license_url}'
            WHERE (
            license_url IS NULL
            AND license = '{license_name}'
            AND license_version = '{license_version}'
            );
            """
        )
        result = postgres.run(update_license_url_query, handler=RETURN_ROW_COUNT)
        if result:
            total_count += result
    logger.info(f"{total_count} image records with missing license_url updated.")
    return total_count


def add_blank_metadata(postgres_conn_id: str):
    """
    Adds blank JSONB object to records where metadata is NULL.
    :param postgres_conn_id: Postgres connection id
    :return:
    """

    logger.info("Replace NULL values in meta_data with blank JSONB object.")

    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    records_without_metadata = postgres.run(
        dedent(
            """
        UPDATE image
        SET meta_data = '{}'
        WHERE meta_data IS NULL;
        """
        ),
        handler=RETURN_ROW_COUNT,
    )
    logger.info(f"{records_without_metadata} records with NULL metadata fixed.")


def move_columns_to_metadata(postgres_conn_id: str):
    columns = [col.WATERMARKED, col.INGESTION_TYPE]
    logger.info(
        f"Moving {','.join([c.db_name for c in columns])} data to the meta_data."
    )

    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    postgres.run(
        dedent(
            """
        UPDATE image
        SET meta_data = meta_data || jsonb_build_object(
            'watermarked', watermarked,
            'ingestion_type', ingestion_type
            )
        WHERE NOT meta_data ?| array['watermarked', 'ingestion_type']
        """
        )
    )


def remove_license_url_from_meta_data(postgres_conn_id: str):
    """
    Removes license_url from meta_data. Iterates over the query result.
    :param postgres_conn_id: Postgres connection id
    :return:
    """

    logger.info("Removing license_url from meta_data.")

    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    postgres.run(
        dedent(
            """
        UPDATE image
        SET meta_data = meta_data - 'license_url'
        WHERE meta_data ? 'license_url' AND license_url IS NOT NULL;
            """
        )
    )


def final_report(postgres_conn_id: str, item_count):
    logger.info(
        "Added license_url to all items. Checking for any records "
        "that still don't have license_url."
    )
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    no_license_url_records = postgres.get_first(
        dedent(
            """
            SELECT COUNT(*) from image WHERE license_url IS NULL
            """
        )
    )[0]
    logger.info(f"There are {no_license_url_records} records without license_url.")
    postgres.run(
        dedent(
            """
            ALTER TABLE image ALTER COLUMN license_url SET NOT NULL;
            """
        )
    )

    items_with_incorrect_metadata = postgres.get_first(
        dedent(
            """
    SELECT COUNT (*)
    FROM image
    WHERE NOT meta_data ?| array['watermarked', 'ingestion_type']
    """
        )
    )[0]
    message = f"""
Added license_url to *{item_count}* items`
Now, there are {no_license_url_records} records without license_url,
and {items_with_incorrect_metadata} records without 'watermarked'
or 'ingestion_type' in the meta_data.
"""
    send_message(message, username="Airflow DAG Data Normalization - license_url")

    logger.info(message)

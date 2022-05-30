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
        "Replacing 100 items' meta_data fields with null, blank jsonb object, "
        "or an object without `license_url` but with "
        "`some_other_property` for testing purposes."
    )
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)

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
    logger.info("Getting image records without license_url.")
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    no_license_url_query = dedent(
        """
        SELECT COUNT(*) from image
        WHERE NOT meta_data ? 'license_url'
        """
    )
    no_license_url_count = postgres.get_first(no_license_url_query)[0]
    logger.info(f"There are {no_license_url_count} records without license_url.")
    is_test = dag_run.conf.get("isTest")
    if is_test:
        return "make_sample_data"
    return "add_blank_metadata"


def update_license_url_batch_query(postgres_conn_id: str):
    """
    Adds license_url to meta_data batching all records with the same license.
    :param postgres_conn_id: Postgres connection id
    :return:
    """

    logger.info("Getting image records without license_url.")
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    total_count = 0
    for license_items, path in license_map.items():
        for license_name, license_version in license_items:
            license_url = f"{base_url}{path}/"
            update_license_url_query = dedent(
                f"""
                UPDATE image
                SET license_url = '{license_url}'
                WHERE ((
                image.meta_data is NOT NULL
                  AND NOT meta_data ?| array['license_url', 'raw_license_url'])
                AND
                  license = '{license_name}' AND license_version = '{license_version}');
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


def update_license_url(postgres_conn_id: str):
    """
    Adds license_url to metadata. Iterates over the query result.
    :param postgres_conn_id: Postgres connection id
    :return:
    """

    logger.info("Adding missing license_url.")

    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    postgres.run(
        dedent(
            """
            ALTER TABLE image
            ADD COLUMN IF NOT EXISTS license_url character varying(200);
            """
        )
    )
    records_without_license_url = postgres.get_records(
        dedent(
            """
        SELECT identifier, license, license_version
        FROM image
        WHERE license_url IS NULL
        """
        )
    )

    total_count = len(records_without_license_url)
    logger.info(f"Will process {total_count} records without license_url.")
    for record in records_without_license_url:
        identifier = record[0]
        license_pair = (record[1], record[2])

        license_url = f"{base_url}{license_map[license_pair]}/"

        postgres.run(
            f"""
        UPDATE image
        SET license_url = '{license_url}'
        WHERE identifier = '{identifier}'"""
        )

    return total_count


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
        SET meta_data = jsonb_set(
            meta_data,
            jsonb_build_object(
            'watermarked', watermarked,
            'ingestion_type', ingestion_type
            ))
        WHERE meta_data ?| array['watermarked', 'ingestion_type']
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
    no_license_url_query = dedent(
        """
        SELECT * from image WHERE license_url IS NULL
        """
    )
    no_license_url_records = postgres.run(
        no_license_url_query, handler=RETURN_ROW_COUNT
    )
    logger.info(f"There are {no_license_url_records} records without license_url.")
    postgres.run(
        dedent(
            """
            ALTER TABLE image ALTER COLUMN license_url SET NOT NULL;
            """
        )
    )
    message = f"""
Added license_url to *{item_count}* items`
Now, there are {no_license_url_records} records without license_url.
"""
    send_message(message, username="Airflow DAG Data Normalization - license_url")

    logger.info(message)
    # This should not run!!!
    if no_license_url_records:
        records_without_license_url = postgres.get_records(no_license_url_query)
        for row in records_without_license_url:
            logger.info(row)

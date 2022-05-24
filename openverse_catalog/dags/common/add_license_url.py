import logging
from textwrap import dedent

from airflow.providers.postgres.hooks.postgres import PostgresHook
from common.licenses.constants import get_reverse_license_path_map


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


def get_statistics(postgres_conn_id: str, **kwargs):
    logger.info("Getting image records without license_url.")
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    no_metadata_query = dedent(
        """
        SELECT COUNT(*) from image WHERE image.meta_data IS NULL
        """
    )
    no_license_url_query = dedent(
        """
        SELECT COUNT(*) from image
        WHERE meta_data IS NOT NULL
        AND NOT meta_data ? 'license_url'
        """
    )
    no_metadata_count = postgres.get_first(no_metadata_query)[0]
    no_license_url_count = postgres.get_first(no_license_url_query)[0]
    logger.info(
        f"There are {no_metadata_count} records without metadata, and "
        f"{no_license_url_count} records without license_url."
    )
    is_test = kwargs["dag_run"].conf.get("isTest")
    if is_test:
        return "make_sample_data"
    return "update_license_url"


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
                SET meta_data = '{{"license_url": "{license_url}"}}'
                WHERE ((
                image.meta_data is NULL OR
                  (image.meta_data is NOT NULL
                  AND NOT meta_data ?| array['license_url', 'raw_license_url']))
                AND
                  license = '{license_name}' AND license_version = '{license_version}');
                """
            )
            result = postgres.run(update_license_url_query, handler=RETURN_ROW_COUNT)
            if result:
                total_count += result
    logger.info(f"{total_count} image records with missing license_url updated.")


def update_license_url(postgres_conn_id: str):
    """
    Adds license_url to metadata. Iterates over the query result.
    :param postgres_conn_id: Postgres connection id
    :return:
    """

    logger.info("Adding missing license_url.")

    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    records_without_license_url = postgres.get_records(
        dedent(
            """
        SELECT identifier, license, license_version
        FROM image
        WHERE (image.meta_data is NULL OR
          (image.meta_data is NOT NULL
          AND NOT meta_data ?| array['license_url', 'raw_license_url'])
        );
        """
        )
    )

    logger.info(
        f"Will process {len(records_without_license_url)} records without license_url."
    )

    for record in records_without_license_url:
        identifier = record[0]
        license_pair = (record[1], record[2])

        new_value = f"""'{{"license_url": "{base_url}{license_map[license_pair]}/"}}'"""

        postgres.run(
            f"""
        UPDATE image
        SET meta_data = CASE WHEN meta_data IS NULL
            THEN {new_value}
            ELSE meta_data || {new_value}
            END
        WHERE identifier = '{identifier}'"""
        )


def final_report(postgres_conn_id: str):
    logger.info(
        "Added license_url to all items. Checking for any records "
        "that still don't have license_url."
    )
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    no_license_url_query = dedent(
        """
            SELECT * from image WHERE NOT meta_data ? 'license_url'
            """
    )
    no_license_url_records = postgres.run(
        no_license_url_query, handler=RETURN_ROW_COUNT
    )
    logger.info(f"There are {no_license_url_records} records without license_url.")
    # This should not run!!!
    if no_license_url_records:
        records_without_license_url = postgres.get_records(no_license_url_query)
        for row in records_without_license_url:
            logger.info(row)

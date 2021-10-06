import json
import logging
from textwrap import dedent
from typing import List

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.errors import InvalidTextRepresentation
from storage import columns as col
from storage.audio import AUDIO_TSV_COLUMNS
from storage.columns import NULL, Column, UpsertStrategy
from storage.db_columns import AUDIO_TABLE_COLUMNS, IMAGE_TABLE_COLUMNS
from storage.image import IMAGE_TSV_COLUMNS, required_columns
from storage.tsv_columns import COLUMNS
from util.constants import AUDIO, IMAGE
from util.loader import provider_details as prov
from util.loader.paths import _extract_media_type


logger = logging.getLogger(__name__)

LOAD_TABLE_NAME_STUB = "provider_data_"
TABLE_NAMES = {AUDIO: AUDIO, IMAGE: IMAGE}
DB_USER_NAME = "deploy"
NOW = "NOW()"
FALSE = "'f'"
OLDEST_PER_PROVIDER = {
    prov.FLICKR_DEFAULT_PROVIDER: "6 months 18 days",
    prov.EUROPEANA_DEFAULT_PROVIDER: "3 months 9 days",
    prov.WIKIMEDIA_DEFAULT_PROVIDER: "6 months 18 days",
    prov.SMITHSONIAN_DEFAULT_PROVIDER: "8 days",
    prov.BROOKLYN_DEFAULT_PROVIDER: "1 month 3 days",
    prov.CLEVELAND_DEFAULT_PROVIDER: "1 month 3 days",
    prov.VICTORIA_DEFAULT_PROVIDER: "1 month 3 days",
    prov.NYPL_DEFAULT_PROVIDER: "1 month 3 days",
    prov.RAWPIXEL_DEFAULT_PROVIDER: "1 month 3 days",
    prov.SCIENCE_DEFAULT_PROVIDER: "1 month 3 days",
    prov.STATENS_DEFAULT_PROVIDER: "1 month 3 days",
}

DB_COLUMNS = {
    IMAGE: IMAGE_TABLE_COLUMNS,
    AUDIO: AUDIO_TABLE_COLUMNS,
}
TSV_COLUMNS = {
    AUDIO: AUDIO_TSV_COLUMNS,
    IMAGE: IMAGE_TSV_COLUMNS,
}
CURRENT_TSV_VERSION = "001"


def create_column_definitions(table_columns: List[Column], is_loading=True):
    """Loading table should not have 'NOT NULL' constraints: all TSV values
    are copied, and then the items without required columns are dropped"""
    definitions = []
    for column in table_columns:
        definition = (
            column.column_definition if is_loading else column.main_column_definition
        )
        definitions.append(definition)
    return ",\n  ".join(definitions)


def create_loading_table(
    postgres_conn_id,
    identifier,
    ti,
):
    """
    Create intermediary table and indices if they do not exist
    """
    media_type = ti.xcom_pull(task_ids="stage_oldest_tsv_file", key="media_type")
    if media_type is None:
        media_type = IMAGE

    load_table = _get_load_table_name(identifier, media_type=media_type)
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    loading_table_columns = TSV_COLUMNS[media_type]
    columns_definition = f"{create_column_definitions(loading_table_columns)}"
    table_creation_query = dedent(
        f"""
    CREATE TABLE public.{load_table}(
    {columns_definition});
    """
    )

    def create_index(column, btree_column=None):
        btree_string = (
            f"{column}"
            if not btree_column
            else f"{btree_column}, md5(({column})::text)"
        )
        postgres.run(
            dedent(
                f"""
               CREATE INDEX IF NOT EXISTS {load_table}_{column}_key
               ON public.{load_table} USING btree ({btree_string});
               """
            )
        )

    postgres.run(table_creation_query)
    postgres.run(f"ALTER TABLE public.{load_table} OWNER TO {DB_USER_NAME};")
    create_index(col.PROVIDER.db_name, None)
    create_index(col.FOREIGN_ID.db_name, "provider")
    create_index(col.DIRECT_URL.db_name, "provider")


def load_local_data_to_intermediate_table(
    postgres_conn_id, tsv_file_name, identifier, max_rows_to_skip=10
):
    media_type = _extract_media_type(tsv_file_name)
    load_table = _get_load_table_name(identifier, media_type=media_type)
    logger.info(f"Loading {tsv_file_name} into {load_table}")

    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    load_successful = False

    while not load_successful and max_rows_to_skip >= 0:
        try:
            postgres.bulk_load(f"{load_table}", tsv_file_name)
            load_successful = True

        except InvalidTextRepresentation as e:
            line_number = _get_malformed_row_in_file(str(e))
            _delete_malformed_row_in_file(tsv_file_name, line_number)

        finally:
            max_rows_to_skip = max_rows_to_skip - 1

    if not load_successful:
        raise InvalidTextRepresentation(
            "Exceeded the maximum number of allowed defective rows"
        )

    _clean_intermediate_table_data(postgres, load_table)


def load_s3_data_to_intermediate_table(
    postgres_conn_id,
    bucket,
    s3_key,
    identifier,
    media_type=IMAGE,
):
    load_table = _get_load_table_name(identifier, media_type=media_type)
    logger.info(f"Loading {s3_key} from S3 Bucket {bucket} into {load_table}")

    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    postgres.run(
        dedent(
            f"""
            SELECT aws_s3.table_import_from_s3(
              '{load_table}',
              '',
              'DELIMITER E''\t''',
              '{bucket}',
              '{s3_key}',
              'us-east-1'
            );
            """
        )
    )
    _clean_intermediate_table_data(postgres, load_table)


def _clean_intermediate_table_data(postgres_hook, load_table):
    """
    Necessary for old TSV files that have not been cleaned up,
    using `MediaStore` class:
    Removes any rows without any of the required fields:
    `url`, `license`, `license_version`, `foreign_id`.
    Also removes any duplicate rows that have the same `provider`
    and `foreign_id`.
    """
    for column in required_columns:
        postgres_hook.run(f"DELETE FROM {load_table} WHERE {column.db_name} IS NULL;")
    postgres_hook.run(
        dedent(
            f"""
            DELETE FROM {load_table} p1
            USING {load_table} p2
            WHERE
              p1.ctid < p2.ctid
              AND p1.{col.PROVIDER.db_name} = p2.{col.PROVIDER.db_name}
              AND p1.{col.FOREIGN_ID.db_name} = p2.{col.FOREIGN_ID.db_name};
            """
        )
    )


def _is_tsv_column_from_different_version(
    column: Column, media_type: str, tsv_version: str
) -> bool:
    """
    Checks that column is a column that exists in TSV files (unlike the db-only
    columns like IDENTIFIER or CREATED_ON), but is not available for `tsv_version`.
    For example, Category column was added to Image TSV in version 001
    >>> from storage.columns import CATEGORY, DIRECT_URL
    >>> _is_tsv_column_from_different_version(CATEGORY, IMAGE, '000')
    True
    >>> _is_tsv_column_from_different_version(DIRECT_URL, IMAGE, '000')
    False
    >>> from storage.columns import IDENTIFIER
    >>> _is_tsv_column_from_different_version(IDENTIFIER, IMAGE, '000')
    False

    """
    return (
        column not in COLUMNS[media_type][tsv_version]
        and column.upsert_strategy == UpsertStrategy.newest_non_null
    )


def upsert_records_to_db_table(
    postgres_conn_id: str,
    identifier: str,
    db_table: str = None,
    media_type: str = IMAGE,
    tsv_version: str = CURRENT_TSV_VERSION,
):
    """
    Upserts newly ingested records from loading table into the main db table.
    For tsv columns that do not exist in the `tsv_version` for `media_type`,
    NULL value is used.
    :param postgres_conn_id
    :param identifier
    :param db_table
    :param media_type
    :param tsv_version:      The version of TSV being processed. This
    determines which columns are used in the upsert query.
    :return:
    """
    if db_table is None:
        db_table = TABLE_NAMES.get(media_type, TABLE_NAMES[IMAGE])

    load_table = _get_load_table_name(identifier, media_type=media_type)
    logger.info(f"Upserting new records into {db_table}.")
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)

    # Remove identifier column
    db_columns: List[Column] = DB_COLUMNS[media_type][1:]
    column_inserts = {}
    column_conflict_values = {}
    for column in db_columns:
        if column.upsert_strategy == UpsertStrategy.no_change:
            column_inserts[column.db_name] = column.upsert_name
        elif _is_tsv_column_from_different_version(column, media_type, tsv_version):
            column_inserts[column.db_name] = NULL
            column_conflict_values[column.db_name] = NULL
        else:
            column_inserts[column.db_name] = column.upsert_name
            column_conflict_values[column.db_name] = column.upsert_value
    upsert_conflict_string = ",\n    ".join(column_conflict_values.values())
    upsert_query = dedent(
        f"""
        INSERT INTO {db_table} AS old ({', '.join(column_inserts.keys())})
        SELECT {', '.join(column_inserts.values())}
        FROM {load_table}
        ON CONFLICT ({col.PROVIDER.db_name}, md5({col.FOREIGN_ID.db_name}))
        DO UPDATE SET
          {upsert_conflict_string}
        """
    )
    postgres.run(upsert_query)


def overwrite_records_in_db_table(
    postgres_conn_id,
    identifier,
    db_table=None,
    media_type=IMAGE,
    tsv_version=CURRENT_TSV_VERSION,
):
    if db_table is None:
        db_table = TABLE_NAMES.get(media_type, TABLE_NAMES[IMAGE])
    load_table = _get_load_table_name(identifier, media_type=media_type)
    logger.info(f"Updating records in {db_table}. {tsv_version}")
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    columns_to_update = TSV_COLUMNS[media_type]
    update_set_string = ",\n  ".join(
        [
            f"{column.db_name} = {load_table}.{column.db_name}"
            for column in columns_to_update
        ]
    )

    update_query = dedent(
        f"""
        UPDATE {db_table}
        SET
        {update_set_string}
        FROM {load_table}
        WHERE
          {db_table}.{col.PROVIDER.db_name} = {load_table}.{col.PROVIDER.db_name}
          AND
          md5({db_table}.{col.FOREIGN_ID.db_name})
            = md5({load_table}.{col.FOREIGN_ID.db_name});
        """
    )
    postgres.run(update_query)


def drop_load_table(postgres_conn_id, identifier, ti):
    media_type = ti.xcom_pull(task_ids="stage_oldest_tsv_file", key="media_type")
    if media_type is None:
        media_type = IMAGE
    load_table = _get_load_table_name(identifier, media_type=media_type)
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    postgres.run(f"DROP TABLE {load_table};")


def _get_load_table_name(
    identifier: str,
    media_type: str = IMAGE,
    load_table_name_stub: str = LOAD_TABLE_NAME_STUB,
) -> str:
    return f"{load_table_name_stub}{media_type}_{identifier}"


def _get_malformed_row_in_file(error_msg):
    error_list = error_msg.splitlines()
    copy_error = next((line for line in error_list if line.startswith("COPY")), None)
    assert copy_error is not None

    line_number = int(copy_error.split("line ")[1].split(",")[0])

    return line_number


def _delete_malformed_row_in_file(tsv_file_name, line_number):
    with open(tsv_file_name, "r") as read_obj:
        lines = read_obj.readlines()

    with open(tsv_file_name, "w") as write_obj:
        for index, line in enumerate(lines):
            if index + 1 != line_number:
                write_obj.write(line)


def _create_temp_flickr_sub_prov_table(
    postgres_conn_id, temp_table="temp_flickr_sub_prov_table"
):
    """
    Drop the temporary table if it already exists
    """
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    postgres.run(f"DROP TABLE IF EXISTS public.{temp_table};")

    """
    Create intermediary table for sub provider migration
    """
    postgres.run(
        dedent(
            f"""
            CREATE TABLE public.{temp_table} (
              {col.CREATOR_URL.db_name} character varying(2000),
              sub_provider character varying(80)
            );
            """
        )
    )

    postgres.run(f"ALTER TABLE public.{temp_table} OWNER TO {DB_USER_NAME};")

    """
    Populate the intermediary table with the sub providers of interest
    """
    for sub_prov, user_id_set in prov.FLICKR_SUB_PROVIDERS.items():
        for user_id in user_id_set:
            creator_url = prov.FLICKR_PHOTO_URL_BASE + user_id
            postgres.run(
                dedent(
                    f"""
                    INSERT INTO public.{temp_table} (
                      {col.CREATOR_URL.db_name},
                      sub_provider
                    )
                    VALUES (
                      '{creator_url}',
                      '{sub_prov}'
                    );
                    """
                )
            )

    return temp_table


def update_flickr_sub_providers(
    postgres_conn_id,
    image_table=TABLE_NAMES[IMAGE],
    default_provider=prov.FLICKR_DEFAULT_PROVIDER,
):
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    temp_table = _create_temp_flickr_sub_prov_table(postgres_conn_id)

    select_query = dedent(
        f"""
        SELECT
        {col.FOREIGN_ID.db_name} AS foreign_id,
        public.{temp_table}.sub_provider AS sub_provider
        FROM {image_table}
        INNER JOIN public.{temp_table}
        ON
        {image_table}.{col.CREATOR_URL.db_name} = public.{temp_table}.{
        col.CREATOR_URL.db_name}
        AND
        {image_table}.{col.PROVIDER.db_name} = '{default_provider}';
        """
    )

    selected_records = postgres.get_records(select_query)
    logger.info(f"Updating {len(selected_records)} records")

    for row in selected_records:
        foreign_id = row[0]
        sub_provider = row[1]
        postgres.run(
            dedent(
                f"""
                UPDATE {image_table}
                SET {col.SOURCE.db_name} = '{sub_provider}'
                WHERE
                {image_table}.{col.PROVIDER.db_name} = '{default_provider}'
                AND
                MD5({image_table}.{col.FOREIGN_ID.db_name}) = MD5('{foreign_id}');
                """
            )
        )

    """
    Drop the temporary table
    """
    postgres.run(f"DROP TABLE public.{temp_table};")


def _create_temp_europeana_sub_prov_table(
    postgres_conn_id, temp_table="temp_eur_sub_prov_table"
):
    """
    Drop the temporary table if it already exists
    """
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    postgres.run(f"DROP TABLE IF EXISTS public.{temp_table};")

    """
    Create intermediary table for sub provider migration
    """
    postgres.run(
        dedent(
            f"""
            CREATE TABLE public.{temp_table} (
              data_provider character varying(120),
              sub_provider character varying(80)
            );
            """
        )
    )

    postgres.run(f"ALTER TABLE public.{temp_table} OWNER TO {DB_USER_NAME};")

    """
    Populate the intermediary table with the sub providers of interest
    """
    for sub_prov, data_provider in prov.EUROPEANA_SUB_PROVIDERS.items():
        postgres.run(
            dedent(
                f"""
                INSERT INTO public.{temp_table} (
                  data_provider,
                  sub_provider
                )
                VALUES (
                  '{data_provider}',
                  '{sub_prov}'
                );
                """
            )
        )

    return temp_table


def update_europeana_sub_providers(
    postgres_conn_id,
    image_table=TABLE_NAMES[IMAGE],
    default_provider=prov.EUROPEANA_DEFAULT_PROVIDER,
    sub_providers=prov.EUROPEANA_SUB_PROVIDERS,
):
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    temp_table = _create_temp_europeana_sub_prov_table(postgres_conn_id)

    select_query = dedent(
        f"""
        SELECT L.foreign_id, L.data_providers, R.sub_provider
        FROM(
        SELECT
        {col.FOREIGN_ID.db_name} AS foreign_id,
        {col.META_DATA.db_name} ->> 'dataProvider' AS data_providers,
        {col.META_DATA.db_name}
        FROM {image_table}
        WHERE {col.PROVIDER.db_name} = '{default_provider}'
        ) L INNER JOIN
        {temp_table} R ON
        L.{col.META_DATA.db_name} ->'dataProvider' ? R.data_provider;
        """
    )

    selected_records = postgres.get_records(select_query)

    """
    Update each selected row if it corresponds to only one sub-provider.
    Otherwise an exception is thrown
    """
    for row in selected_records:
        foreign_id = row[0]
        data_providers = json.loads(row[1])
        sub_provider = row[2]

        eligible_sub_providers = {
            s for s in sub_providers if sub_providers[s] in data_providers
        }
        if len(eligible_sub_providers) > 1:
            raise Exception(
                f"More than one sub-provider identified for the "
                f"image with foreign ID {foreign_id}"
            )

        assert len(eligible_sub_providers) == 1
        assert eligible_sub_providers.pop() == sub_provider

        postgres.run(
            dedent(
                f"""
                UPDATE {image_table}
                SET {col.SOURCE.db_name} = '{sub_provider}'
                WHERE
                {image_table}.{col.PROVIDER.db_name} = '{default_provider}'
                AND
                MD5({image_table}.{col.FOREIGN_ID.db_name}) = MD5('{foreign_id}');
                """
            )
        )

    """
    Drop the temporary table
    """
    postgres.run(f"DROP TABLE public.{temp_table};")


def update_smithsonian_sub_providers(
    postgres_conn_id,
    image_table=TABLE_NAMES[IMAGE],
    default_provider=prov.SMITHSONIAN_DEFAULT_PROVIDER,
    sub_providers=prov.SMITHSONIAN_SUB_PROVIDERS,
):
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)

    """
    Select all records where the source value is not yet updated
    """
    select_query = dedent(
        f"""
        SELECT {col.FOREIGN_ID.db_name},
        {col.META_DATA.db_name} ->> 'unit_code' AS unit_code
        FROM {image_table}
        WHERE
        {col.PROVIDER.db_name} = '{default_provider}'
        AND
        {col.SOURCE.db_name} = '{default_provider}';
        """
    )

    selected_records = postgres.get_records(select_query)

    """
    Set the source value of each selected row to the sub-provider value
    corresponding to unit code. If the unit code is unknown, an error is thrown
    """
    for row in selected_records:
        foreign_id = row[0]
        unit_code = row[1]

        source = next((s for s in sub_providers if unit_code in sub_providers[s]), None)
        if source is None:
            raise Exception(f"An unknown unit code value {unit_code} encountered ")

        postgres.run(
            dedent(
                f"""
                UPDATE {image_table}
                SET {col.SOURCE.db_name} = '{source}'
                WHERE
                {image_table}.{col.PROVIDER.db_name} = '{default_provider}'
                AND
                MD5({image_table}.{col.FOREIGN_ID.db_name}) = MD5('{foreign_id}');
                """
            )
        )


def expire_old_images(postgres_conn_id, provider, image_table=TABLE_NAMES[IMAGE]):
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)

    if provider not in OLDEST_PER_PROVIDER:
        raise Exception(
            f"Provider value {provider} not defined in the "
            f"OLDEST_PER_PROVIDER dictionary"
        )

    """
    Select all records that are outdated
    """
    select_query = dedent(
        f"""
        SELECT {col.FOREIGN_ID.db_name}
        FROM {image_table}
        WHERE
        {col.PROVIDER.db_name} = '{provider}'
        AND
        {col.UPDATED_ON.db_name} < {NOW} - INTERVAL '{OLDEST_PER_PROVIDER[provider]}';
        """
    )

    selected_records = postgres.get_records(select_query)

    """
    Set the 'removed_from_source' value of each selected row to True to
    indicate that those images are outdated
    """
    for row in selected_records:
        foreign_id = row[0]

        postgres.run(
            dedent(
                f"""
                UPDATE {image_table}
                SET {col.REMOVED.db_name} = 't'
                WHERE
                {image_table}.{col.PROVIDER.db_name} = '{provider}'
                AND
                MD5({image_table}.{col.FOREIGN_ID.db_name}) = MD5('{foreign_id}');
                """
            )
        )

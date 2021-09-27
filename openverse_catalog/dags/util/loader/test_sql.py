import json
import logging
import os
import socket
import time
from collections import namedtuple
from datetime import datetime
from urllib.parse import urlparse

import boto3
import psycopg2
import pytest
from airflow.models import TaskInstance
from airflow.operators.dummy import DummyOperator
from psycopg2.errors import InvalidTextRepresentation
from storage import column_names as col
from storage.columns import (
    CREATOR_COLUMN,
    CREATOR_URL_COLUMN,
    DIRECT_URL_COLUMN,
    FILESIZE_COLUMN,
    FOREIGN_ID_COLUMN,
    HEIGHT_COLUMN,
    INGESTION_TYPE_COLUMN,
    LANDING_URL_COLUMN,
    LAST_SYNCED_COLUMN,
    LICENSE_COLUMN,
    LICENSE_VERSION_COLUMN,
    META_DATA_COLUMN,
    PROVIDER_COLUMN,
    REMOVED_COLUMN,
    SOURCE_COLUMN,
    TAGS_COLUMN,
    THUMBNAIL_COLUMN,
    TITLE_COLUMN,
    UPDATED_ON_COLUMN,
    WATERMARKED_COLUMN,
    WIDTH_COLUMN,
)
from storage.db_table import IMAGE_TABLE_COLUMNS
from util.constants import IMAGE
from util.loader import sql
from util.loader.sql import TSV_COLUMNS, create_column_definitions


TEST_ID = "testing"
POSTGRES_CONN_ID = os.getenv("TEST_CONN_ID")
POSTGRES_TEST_URI = os.getenv("AIRFLOW_CONN_POSTGRES_OPENLEDGER_TESTING")
TEST_LOAD_TABLE = f"provider_data_image_{TEST_ID}"
TEST_IMAGE_TABLE = f"image_{TEST_ID}"
S3_LOCAL_ENDPOINT = os.getenv("S3_LOCAL_ENDPOINT")
S3_TEST_BUCKET = f"cccatalog-storage-{TEST_ID}"
ACCESS_KEY = os.getenv("TEST_ACCESS_KEY")
SECRET_KEY = os.getenv("TEST_SECRET_KEY")
S3_HOST = socket.gethostbyname(urlparse(S3_LOCAL_ENDPOINT).hostname)

RESOURCES = os.path.join(os.path.abspath(os.path.dirname(__file__)), "test_resources")

DROP_LOAD_TABLE_QUERY = f"DROP TABLE IF EXISTS {TEST_LOAD_TABLE} CASCADE;"
DROP_IMAGE_TABLE_QUERY = f"DROP TABLE IF EXISTS {TEST_IMAGE_TABLE} CASCADE;"

LOADING_TABLE_COLUMN_DEFINITIONS = create_column_definitions(
    TSV_COLUMNS[IMAGE], is_loading=True
)

CREATE_LOAD_TABLE_QUERY = f"""CREATE TABLE public.{TEST_LOAD_TABLE} (
  {LOADING_TABLE_COLUMN_DEFINITIONS}
);"""

UUID_FUNCTION_QUERY = 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;'

IMAGE_TABLE_COLUMN_DEFINITIONS = create_column_definitions(IMAGE_TABLE_COLUMNS)

CREATE_IMAGE_TABLE_QUERY = f"""CREATE TABLE public.{TEST_IMAGE_TABLE} (
  {IMAGE_TABLE_COLUMN_DEFINITIONS}
);"""

UNIQUE_CONDITION_QUERY = (
    f"CREATE UNIQUE INDEX {TEST_IMAGE_TABLE}_provider_fid_idx"
    f" ON public.{TEST_IMAGE_TABLE}"
    " USING btree (provider, md5(foreign_identifier));"
)

DROP_IMAGE_INDEX_QUERY = f"DROP INDEX IF EXISTS {TEST_IMAGE_TABLE}_provider_fid_idx;"

op_no_dag = DummyOperator(task_id="op_no_dag")
naive_datetime = datetime(2016, 1, 1).replace(tzinfo=None)
ti = TaskInstance(task=op_no_dag, execution_date=naive_datetime)

# ids for main database columns
updated_idx = IMAGE_TABLE_COLUMNS.index(UPDATED_ON_COLUMN)
ingestion_idx = IMAGE_TABLE_COLUMNS.index(INGESTION_TYPE_COLUMN)
provider_idx = IMAGE_TABLE_COLUMNS.index(PROVIDER_COLUMN)
source_idx = IMAGE_TABLE_COLUMNS.index(SOURCE_COLUMN)
fid_idx = IMAGE_TABLE_COLUMNS.index(FOREIGN_ID_COLUMN)
land_url_idx = IMAGE_TABLE_COLUMNS.index(LANDING_URL_COLUMN)
url_idx = IMAGE_TABLE_COLUMNS.index(DIRECT_URL_COLUMN)
thm_idx = IMAGE_TABLE_COLUMNS.index(THUMBNAIL_COLUMN)
filesize_idx = IMAGE_TABLE_COLUMNS.index(FILESIZE_COLUMN)
license_idx = IMAGE_TABLE_COLUMNS.index(LICENSE_COLUMN)
version_idx = IMAGE_TABLE_COLUMNS.index(LICENSE_VERSION_COLUMN)
creator_idx = IMAGE_TABLE_COLUMNS.index(CREATOR_COLUMN)
creator_url_idx = IMAGE_TABLE_COLUMNS.index(CREATOR_URL_COLUMN)
title_idx = IMAGE_TABLE_COLUMNS.index(TITLE_COLUMN)
metadata_idx = IMAGE_TABLE_COLUMNS.index(META_DATA_COLUMN)
tags_idx = IMAGE_TABLE_COLUMNS.index(TAGS_COLUMN)
synced_idx = IMAGE_TABLE_COLUMNS.index(LAST_SYNCED_COLUMN)
removed_idx = IMAGE_TABLE_COLUMNS.index(REMOVED_COLUMN)
watermarked_idx = IMAGE_TABLE_COLUMNS.index(WATERMARKED_COLUMN)
width_idx = IMAGE_TABLE_COLUMNS.index(WIDTH_COLUMN)
height_idx = IMAGE_TABLE_COLUMNS.index(HEIGHT_COLUMN)


def create_query_values(
    column_values: dict,
    columns=None,
):
    if columns is None:
        columns = TSV_COLUMNS[IMAGE]
    result = []
    for column in columns:
        val = column_values.get(column.db_name)
        if val is None:
            val = "null"
        else:
            val = f"'{str(val)}'"
        result.append(val)
    return ",".join(result)


@pytest.fixture
def postgres():
    Postgres = namedtuple("Postgres", ["cursor", "connection"])
    conn = psycopg2.connect(POSTGRES_TEST_URI)
    cur = conn.cursor()
    drop_command = f"DROP TABLE IF EXISTS {TEST_LOAD_TABLE}"
    cur.execute(drop_command)
    conn.commit()

    yield Postgres(cursor=cur, connection=conn)

    cur.execute(drop_command)
    cur.close()
    conn.commit()
    conn.close()


@pytest.fixture
def postgres_with_load_table():
    Postgres = namedtuple("Postgres", ["cursor", "connection"])
    conn = psycopg2.connect(POSTGRES_TEST_URI)
    cur = conn.cursor()
    drop_command = f"DROP TABLE IF EXISTS {TEST_LOAD_TABLE}"
    cur.execute(drop_command)
    conn.commit()
    create_command = CREATE_LOAD_TABLE_QUERY
    cur.execute(create_command)
    conn.commit()

    yield Postgres(cursor=cur, connection=conn)

    cur.execute(drop_command)
    cur.close()
    conn.commit()
    conn.close()


@pytest.fixture
def postgres_with_load_and_image_table():
    Postgres = namedtuple("Postgres", ["cursor", "connection"])
    conn = psycopg2.connect(POSTGRES_TEST_URI)
    cur = conn.cursor()

    logging.info(f"dropping load table: {DROP_LOAD_TABLE_QUERY}")
    logging.info(f"dropping image table: {DROP_IMAGE_TABLE_QUERY}")
    cur.execute(DROP_LOAD_TABLE_QUERY)
    cur.execute(DROP_IMAGE_TABLE_QUERY)
    cur.execute(DROP_IMAGE_INDEX_QUERY)
    cur.execute(CREATE_LOAD_TABLE_QUERY)
    cur.execute(UUID_FUNCTION_QUERY)
    cur.execute(CREATE_IMAGE_TABLE_QUERY)
    cur.execute(UNIQUE_CONDITION_QUERY)

    conn.commit()

    yield Postgres(cursor=cur, connection=conn)

    cur.execute(DROP_LOAD_TABLE_QUERY)
    cur.execute(DROP_IMAGE_TABLE_QUERY)
    cur.execute(DROP_IMAGE_INDEX_QUERY)
    cur.close()
    conn.commit()
    conn.close()


@pytest.fixture
def empty_s3_bucket(socket_enabled):
    bucket = boto3.resource(
        "s3",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url=S3_LOCAL_ENDPOINT,
    ).Bucket(S3_TEST_BUCKET)

    def _delete_all_objects():
        key_list = [{"Key": obj.key} for obj in bucket.objects.all()]
        if len(list(bucket.objects.all())) > 0:
            bucket.delete_objects(Delete={"Objects": key_list})

    if bucket.creation_date:
        _delete_all_objects()
    else:
        bucket.create()
    yield bucket
    _delete_all_objects()


def _load_local_tsv(tmpdir, bucket, tsv_file_name):
    """
    This wraps sql.load_local_data_to_intermediate_table so we can test it
    under various conditions.
    """
    tsv_file_path = os.path.join(RESOURCES, tsv_file_name)
    with open(tsv_file_path) as f:
        f_data = f.read()

    test_tsv = "test.tsv"
    path = tmpdir.join(test_tsv)
    path.write(f_data)

    sql.load_local_data_to_intermediate_table(POSTGRES_CONN_ID, str(path), TEST_ID)


def _load_s3_tsv(tmpdir, bucket, tsv_file_name):
    tsv_file_path = os.path.join(RESOURCES, tsv_file_name)
    key = "path/to/object/{tsv_file_name}"
    bucket.upload_file(tsv_file_path, key)
    sql.load_s3_data_to_intermediate_table(
        POSTGRES_CONN_ID, S3_TEST_BUCKET, key, TEST_ID
    )


def test_create_loading_table_creates_table(postgres):
    postgres_conn_id = POSTGRES_CONN_ID
    identifier = TEST_ID
    load_table = TEST_LOAD_TABLE
    sql.create_loading_table(postgres_conn_id, identifier, ti)

    check_query = (
        f"SELECT EXISTS (" f"SELECT FROM pg_tables WHERE tablename='{load_table}');"
    )
    postgres.cursor.execute(check_query)
    check_result = postgres.cursor.fetchone()[0]
    assert check_result


def test_create_loading_table_errors_if_run_twice_with_same_id(postgres):
    postgres_conn_id = POSTGRES_CONN_ID
    identifier = TEST_ID
    sql.create_loading_table(postgres_conn_id, identifier, ti)
    with pytest.raises(Exception):
        sql.create_loading_table(postgres_conn_id, identifier, ti)


@pytest.mark.parametrize("load_function", [_load_local_tsv, _load_s3_tsv])
@pytest.mark.allow_hosts([S3_HOST])
def test_loaders_load_good_tsv(
    postgres_with_load_table, tmpdir, empty_s3_bucket, load_function
):
    load_function(tmpdir, empty_s3_bucket, "none_missing.tsv")
    check_query = f"SELECT COUNT (*) FROM {TEST_LOAD_TABLE};"
    postgres_with_load_table.cursor.execute(check_query)
    num_rows = postgres_with_load_table.cursor.fetchone()[0]
    assert num_rows == 10


@pytest.mark.parametrize("load_function", [_load_local_tsv])
def test_delete_less_than_max_malformed_rows(
    postgres_with_load_table, tmpdir, empty_s3_bucket, load_function
):
    load_function(tmpdir, empty_s3_bucket, "malformed_less_than_max_rows.tsv")
    check_query = f"SELECT COUNT (*) FROM {TEST_LOAD_TABLE};"
    postgres_with_load_table.cursor.execute(check_query)
    num_rows = postgres_with_load_table.cursor.fetchone()[0]
    assert num_rows == 6


@pytest.mark.parametrize("load_function", [_load_local_tsv])
def test_delete_max_malformed_rows(
    postgres_with_load_table, tmpdir, empty_s3_bucket, load_function
):
    load_function(tmpdir, empty_s3_bucket, "malformed_max_rows.tsv")
    check_query = f"SELECT COUNT (*) FROM {TEST_LOAD_TABLE};"
    postgres_with_load_table.cursor.execute(check_query)
    num_rows = postgres_with_load_table.cursor.fetchone()[0]
    assert num_rows == 3


@pytest.mark.parametrize("load_function", [_load_local_tsv])
def test_delete_more_than_max_malformed_rows(
    postgres_with_load_table, tmpdir, empty_s3_bucket, load_function
):
    with pytest.raises(InvalidTextRepresentation):
        load_function(tmpdir, empty_s3_bucket, "malformed_more_than_max_rows.tsv")


@pytest.mark.parametrize("load_function", [_load_local_tsv, _load_s3_tsv])
@pytest.mark.allow_hosts([S3_HOST])
def test_loaders_delete_null_url_rows(
    postgres_with_load_table, tmpdir, empty_s3_bucket, load_function
):
    load_function(tmpdir, empty_s3_bucket, "url_missing.tsv")
    null_url_check = f"SELECT COUNT (*) FROM {TEST_LOAD_TABLE} WHERE url IS NULL;"
    postgres_with_load_table.cursor.execute(null_url_check)
    null_url_num_rows = postgres_with_load_table.cursor.fetchone()[0]
    remaining_row_count = f"SELECT COUNT (*) FROM {TEST_LOAD_TABLE};"
    postgres_with_load_table.cursor.execute(remaining_row_count)
    remaining_rows = postgres_with_load_table.cursor.fetchone()[0]

    assert null_url_num_rows == 0
    assert remaining_rows == 2


@pytest.mark.parametrize("load_function", [_load_local_tsv, _load_s3_tsv])
@pytest.mark.allow_hosts([S3_HOST])
def test_loaders_delete_null_license_rows(
    postgres_with_load_table, tmpdir, empty_s3_bucket, load_function
):
    load_function(tmpdir, empty_s3_bucket, "license_missing.tsv")
    license_check = f"SELECT COUNT (*) FROM {TEST_LOAD_TABLE} WHERE license IS NULL;"
    postgres_with_load_table.cursor.execute(license_check)
    null_license_num_rows = postgres_with_load_table.cursor.fetchone()[0]
    remaining_row_count = f"SELECT COUNT (*) FROM {TEST_LOAD_TABLE};"
    postgres_with_load_table.cursor.execute(remaining_row_count)
    remaining_rows = postgres_with_load_table.cursor.fetchone()[0]

    assert null_license_num_rows == 0
    assert remaining_rows == 2


@pytest.mark.parametrize("load_function", [_load_local_tsv, _load_s3_tsv])
@pytest.mark.allow_hosts([S3_HOST])
def test_loaders_delete_null_foreign_landing_url_rows(
    postgres_with_load_table, tmpdir, empty_s3_bucket, load_function
):
    load_function(tmpdir, empty_s3_bucket, "foreign_landing_url_missing.tsv")
    foreign_landing_url_check = (
        f"SELECT COUNT (*) FROM {TEST_LOAD_TABLE} "
        f"WHERE foreign_landing_url IS NULL;"
    )
    postgres_with_load_table.cursor.execute(foreign_landing_url_check)
    null_foreign_landing_url_num_rows = postgres_with_load_table.cursor.fetchone()[0]
    remaining_row_count = f"SELECT COUNT (*) FROM {TEST_LOAD_TABLE};"
    postgres_with_load_table.cursor.execute(remaining_row_count)
    remaining_rows = postgres_with_load_table.cursor.fetchone()[0]

    assert null_foreign_landing_url_num_rows == 0
    assert remaining_rows == 3


@pytest.mark.parametrize("load_function", [_load_local_tsv, _load_s3_tsv])
@pytest.mark.allow_hosts([S3_HOST])
def test_data_loaders_delete_null_foreign_identifier_rows(
    postgres_with_load_table, tmpdir, empty_s3_bucket, load_function
):
    load_function(tmpdir, empty_s3_bucket, "foreign_identifier_missing.tsv")
    foreign_identifier_check = (
        f"SELECT COUNT (*) FROM {TEST_LOAD_TABLE} " f"WHERE foreign_identifier IS NULL;"
    )
    postgres_with_load_table.cursor.execute(foreign_identifier_check)
    null_foreign_identifier_num_rows = postgres_with_load_table.cursor.fetchone()[0]
    remaining_row_count = f"SELECT COUNT (*) FROM {TEST_LOAD_TABLE};"
    postgres_with_load_table.cursor.execute(remaining_row_count)
    remaining_rows = postgres_with_load_table.cursor.fetchone()[0]

    assert null_foreign_identifier_num_rows == 0
    assert remaining_rows == 1


@pytest.mark.parametrize("load_function", [_load_local_tsv, _load_s3_tsv])
@pytest.mark.allow_hosts([S3_HOST])
def test_import_data_deletes_duplicate_foreign_identifier_rows(
    postgres_with_load_table, tmpdir, empty_s3_bucket, load_function
):
    load_function(tmpdir, empty_s3_bucket, "foreign_identifier_duplicate.tsv")
    foreign_id_duplicate_check = (
        f"SELECT COUNT (*) FROM {TEST_LOAD_TABLE} "
        f"WHERE foreign_identifier='135257';"
    )
    postgres_with_load_table.cursor.execute(foreign_id_duplicate_check)
    foreign_id_duplicate_num_rows = postgres_with_load_table.cursor.fetchone()[0]
    remaining_row_count = f"SELECT COUNT (*) FROM {TEST_LOAD_TABLE};"
    postgres_with_load_table.cursor.execute(remaining_row_count)
    remaining_rows = postgres_with_load_table.cursor.fetchone()[0]

    assert foreign_id_duplicate_num_rows == 1
    assert remaining_rows == 3


def test_upsert_records_inserts_one_record_to_empty_image_table(
    postgres_with_load_and_image_table, tmpdir
):
    postgres_conn_id = POSTGRES_CONN_ID
    load_table = TEST_LOAD_TABLE
    image_table = TEST_IMAGE_TABLE
    identifier = TEST_ID

    FID = "a"
    LAND_URL = "https://images.com/a"
    IMG_URL = "https://images.com/a/img.jpg"
    THM_URL = "https://images.com/a/img_small.jpg"
    WIDTH = 1000
    HEIGHT = 500
    FILESIZE = 2000
    LICENSE = "cc0"
    VERSION = "1.0"
    CREATOR = "Alice"
    CREATOR_URL = "https://alice.com"
    TITLE = "My Great Pic"
    META_DATA = '{"description": "what a cool picture"}'
    TAGS = '["fun", "great"]'
    WATERMARKED = "f"
    PROVIDER = "images_provider"
    SOURCE = "images_source"
    INGESTION_TYPE = "test_ingestion"

    query_values = create_query_values(
        {
            col.FOREIGN_ID: FID,
            col.LANDING_URL: LAND_URL,
            col.DIRECT_URL: IMG_URL,
            col.THUMBNAIL: THM_URL,
            col.FILESIZE: FILESIZE,
            col.LICENSE: LICENSE,
            col.LICENSE_VERSION: VERSION,
            col.CREATOR: CREATOR,
            col.CREATOR_URL: CREATOR_URL,
            col.TITLE: TITLE,
            col.META_DATA: META_DATA,
            col.TAGS: TAGS,
            col.WATERMARKED: WATERMARKED,
            col.PROVIDER: PROVIDER,
            col.SOURCE: SOURCE,
            col.INGESTION_TYPE: INGESTION_TYPE,
            col.WIDTH: WIDTH,
            col.HEIGHT: HEIGHT,
        }
    )
    load_data_query = f"""INSERT INTO {load_table} VALUES(
        {query_values}
        );"""
    postgres_with_load_and_image_table.cursor.execute(load_data_query)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.cursor.execute(f"SELECT * FROM {image_table};")
    actual_rows = postgres_with_load_and_image_table.cursor.fetchall()
    actual_row = actual_rows[0]
    assert len(actual_rows) == 1
    assert actual_row[ingestion_idx] == INGESTION_TYPE
    assert actual_row[provider_idx] == PROVIDER
    assert actual_row[source_idx] == SOURCE
    assert actual_row[fid_idx] == FID
    assert actual_row[land_url_idx] == LAND_URL
    assert actual_row[url_idx] == IMG_URL
    assert actual_row[thm_idx] == THM_URL
    assert actual_row[filesize_idx] == FILESIZE
    assert actual_row[license_idx] == LICENSE
    assert actual_row[version_idx] == VERSION
    assert actual_row[creator_idx] == CREATOR
    assert actual_row[creator_url_idx] == CREATOR_URL
    assert actual_row[title_idx] == TITLE
    assert actual_row[metadata_idx] == json.loads(META_DATA)
    assert actual_row[tags_idx] == json.loads(TAGS)
    assert actual_row[watermarked_idx] is False
    assert actual_row[width_idx] == WIDTH
    assert actual_row[height_idx] == HEIGHT


def test_upsert_records_inserts_two_records_to_image_table(
    postgres_with_load_and_image_table, tmpdir
):
    postgres_conn_id = POSTGRES_CONN_ID
    load_table = TEST_LOAD_TABLE
    image_table = TEST_IMAGE_TABLE
    identifier = TEST_ID

    FID_A = "a"
    FID_B = "b"
    LAND_URL_A = "https://images.com/a"
    LAND_URL_B = "https://images.com/b"
    IMG_URL_A = "images.com/a/img.jpg"
    IMG_URL_B = "images.com/b/img.jpg"
    LICENSE = "cc0"
    VERSION = "1.0"
    PROVIDER = "images"

    test_rows = [
        (FID_A, LAND_URL_A, IMG_URL_A, LICENSE, VERSION, PROVIDER),
        (FID_B, LAND_URL_B, IMG_URL_B, LICENSE, VERSION, PROVIDER),
    ]

    for r in test_rows:
        load_data_query = f"""INSERT INTO {load_table} (
            foreign_identifier, foreign_landing_url, url,
             license, license_version, provider, source
            ) VALUES (
            '{r[0]}', '{r[1]}', '{r[2]}',
            '{r[3]}', '{r[4]}', '{r[5]}', '{r[5]}'
            );"""
        postgres_with_load_and_image_table.cursor.execute(load_data_query)
        postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.cursor.execute(f"SELECT * FROM {image_table};")
    actual_rows = postgres_with_load_and_image_table.cursor.fetchall()
    assert actual_rows[0][fid_idx] == FID_A
    assert actual_rows[1][fid_idx] == FID_B


def test_upsert_records_replaces_updated_on_and_last_synced_with_source(
    postgres_with_load_and_image_table, tmpdir
):
    postgres_conn_id = POSTGRES_CONN_ID
    load_table = TEST_LOAD_TABLE
    image_table = TEST_IMAGE_TABLE
    identifier = TEST_ID

    FID = "a"
    LAND_URL = "https://images.com/a"
    IMG_URL = "images.com/a/img.jpg"
    LICENSE = "cc0"
    VERSION = "1.0"
    PROVIDER = "images"

    load_data_query = f"""INSERT INTO {load_table} (
        foreign_identifier, foreign_landing_url, url,
         license, license_version, provider, source
        ) VALUES (
        '{FID}','{LAND_URL}','{IMG_URL}','{LICENSE}','{VERSION}',
        '{PROVIDER}','{PROVIDER}'
        );"""
    postgres_with_load_and_image_table.cursor.execute(load_data_query)
    postgres_with_load_and_image_table.connection.commit()

    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.cursor.execute(f"SELECT * FROM {image_table};")
    original_row = postgres_with_load_and_image_table.cursor.fetchall()[0]
    original_updated_on = original_row[updated_idx]
    original_last_synced = original_row[synced_idx]

    time.sleep(0.01)
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.cursor.execute(f"SELECT * FROM {image_table};")
    updated_result = postgres_with_load_and_image_table.cursor.fetchall()
    updated_row = updated_result[0]
    updated_updated_on = updated_row[updated_idx]
    updated_last_synced = updated_row[synced_idx]

    assert len(updated_result) == 1
    assert updated_updated_on > original_updated_on
    assert updated_last_synced > original_last_synced


def test_upsert_records_replaces_data(postgres_with_load_and_image_table, tmpdir):
    postgres_conn_id = POSTGRES_CONN_ID
    load_table = TEST_LOAD_TABLE
    image_table = TEST_IMAGE_TABLE
    identifier = TEST_ID

    FID = "a"
    PROVIDER = "images_provider"
    SOURCE = "images_source"
    WATERMARKED = "f"
    FILESIZE = 2000
    TAGS = '["fun", "great"]'

    IMG_URL_A = "https://images.com/a/img.jpg"
    LAND_URL_A = "https://images.com/a"
    THM_URL_A = "https://images.com/a/img_small.jpg"
    WIDTH_A = 1000
    HEIGHT_A = 500
    LICENSE_A = "by"
    VERSION_A = "4.0"
    CREATOR_A = "Alice"
    CREATOR_URL_A = "https://alice.com"
    TITLE_A = "My Great Pic"
    META_DATA_A = '{"description": "what a cool picture"}'
    INGESTION_TYPE = "provider_api"

    IMG_URL_B = "https://images.com/b/img.jpg"
    LAND_URL_B = "https://images.com/b"
    THM_URL_B = "https://images.com/b/img_small.jpg"
    WIDTH_B = 2000
    HEIGHT_B = 1000
    LICENSE_B = "cc0"
    VERSION_B = "1.0"
    CREATOR_B = "Bob"
    CREATOR_URL_B = "https://bob.com"
    TITLE_B = "Bobs Great Pic"
    META_DATA_B = '{"description": "Bobs cool picture"}'

    query_values = create_query_values(
        {
            col.FOREIGN_ID: FID,
            col.LANDING_URL: LAND_URL_A,
            col.DIRECT_URL: IMG_URL_A,
            col.THUMBNAIL: THM_URL_A,
            col.FILESIZE: FILESIZE,
            col.LICENSE: LICENSE_A,
            col.LICENSE_VERSION: VERSION_A,
            col.CREATOR: CREATOR_A,
            col.CREATOR_URL: CREATOR_URL_A,
            col.TITLE: TITLE_A,
            col.META_DATA: META_DATA_A,
            col.TAGS: TAGS,
            col.WATERMARKED: WATERMARKED,
            col.PROVIDER: PROVIDER,
            col.SOURCE: SOURCE,
            col.INGESTION_TYPE: INGESTION_TYPE,
            col.WIDTH: WIDTH_A,
            col.HEIGHT: HEIGHT_A,
        }
    )
    load_data_query_a = f"""INSERT INTO {load_table} VALUES(
       {query_values}
       );"""
    postgres_with_load_and_image_table.cursor.execute(load_data_query_a)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()

    query_values = create_query_values(
        {
            col.FOREIGN_ID: FID,
            col.LANDING_URL: LAND_URL_B,
            col.DIRECT_URL: IMG_URL_B,
            col.THUMBNAIL: THM_URL_B,
            col.FILESIZE: FILESIZE,
            col.LICENSE: LICENSE_B,
            col.LICENSE_VERSION: VERSION_B,
            col.CREATOR: CREATOR_B,
            col.CREATOR_URL: CREATOR_URL_B,
            col.TITLE: TITLE_B,
            col.META_DATA: META_DATA_B,
            col.TAGS: TAGS,
            col.WATERMARKED: WATERMARKED,
            col.PROVIDER: PROVIDER,
            col.SOURCE: SOURCE,
            col.INGESTION_TYPE: INGESTION_TYPE,
            col.WIDTH: WIDTH_B,
            col.HEIGHT: HEIGHT_B,
        }
    )
    load_data_query_b = f"""INSERT INTO {load_table} VALUES(
        {query_values}
        );"""
    postgres_with_load_and_image_table.cursor.execute(f"DELETE FROM {load_table};")
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(load_data_query_b)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"SELECT * FROM {image_table};")
    actual_rows = postgres_with_load_and_image_table.cursor.fetchall()
    actual_row = actual_rows[0]
    assert len(actual_rows) == 1
    assert actual_row[land_url_idx] == LAND_URL_B
    assert actual_row[url_idx] == IMG_URL_B
    assert actual_row[thm_idx] == THM_URL_B
    assert actual_row[license_idx] == LICENSE_B
    assert actual_row[version_idx] == VERSION_B
    assert actual_row[creator_idx] == CREATOR_B
    assert actual_row[creator_url_idx] == CREATOR_URL_B
    assert actual_row[title_idx] == TITLE_B
    assert actual_row[metadata_idx] == json.loads(META_DATA_B)
    assert actual_row[tags_idx] == json.loads(TAGS)
    assert actual_row[width_idx] == WIDTH_B


def test_upsert_records_does_not_replace_with_nulls(
    postgres_with_load_and_image_table, tmpdir
):
    postgres_conn_id = POSTGRES_CONN_ID
    load_table = TEST_LOAD_TABLE
    image_table = TEST_IMAGE_TABLE
    identifier = TEST_ID

    FID = "a"
    PROVIDER = "images_provider"
    SOURCE = "images_source"
    WATERMARKED = "f"
    IMG_URL = "https://images.com/a/img.jpg"
    FILESIZE = 2000
    TAGS = '["fun", "great"]'
    INGESTION_TYPE = "provider_api"

    LAND_URL_A = "https://images.com/a"
    THM_URL_A = "https://images.com/a/img_small.jpg"
    WIDTH_A = 1000
    HEIGHT_A = 500
    LICENSE_A = "by"
    VERSION_A = "4.0"
    CREATOR_A = "Alice"
    CREATOR_URL_A = "https://alice.com"
    TITLE_A = "My Great Pic"
    META_DATA_A = '{"description": "what a cool picture"}'

    LAND_URL_B = "https://images.com/b"
    LICENSE_B = "cc0"
    VERSION_B = "1.0"

    query_values_a = create_query_values(
        {
            col.FOREIGN_ID: FID,
            col.LANDING_URL: LAND_URL_A,
            col.DIRECT_URL: IMG_URL,
            col.THUMBNAIL: THM_URL_A,
            col.FILESIZE: FILESIZE,
            col.LICENSE: LICENSE_A,
            col.LICENSE_VERSION: VERSION_A,
            col.CREATOR: CREATOR_A,
            col.CREATOR_URL: CREATOR_URL_A,
            col.TITLE: TITLE_A,
            col.META_DATA: META_DATA_A,
            col.TAGS: TAGS,
            col.WATERMARKED: WATERMARKED,
            col.PROVIDER: PROVIDER,
            col.SOURCE: SOURCE,
            col.INGESTION_TYPE: INGESTION_TYPE,
            col.WIDTH: WIDTH_A,
            col.HEIGHT: HEIGHT_A,
        }
    )
    load_data_query_a = f"""INSERT INTO {load_table} VALUES(
        {query_values_a}
        );"""
    postgres_with_load_and_image_table.cursor.execute(load_data_query_a)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()

    query_values_b = create_query_values(
        {
            col.FOREIGN_ID: FID,
            col.LANDING_URL: LAND_URL_B,
            col.DIRECT_URL: IMG_URL,
            col.LICENSE: LICENSE_B,
            col.LICENSE_VERSION: VERSION_B,
            col.TAGS: TAGS,
            col.PROVIDER: PROVIDER,
            col.SOURCE: SOURCE,
        }
    )
    load_data_query_b = f"""INSERT INTO {load_table} VALUES(
        {query_values_b}
        );"""
    postgres_with_load_and_image_table.cursor.execute(f"DELETE FROM {load_table};")
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(load_data_query_b)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"SELECT * FROM {image_table};")
    actual_rows = postgres_with_load_and_image_table.cursor.fetchall()
    actual_row = actual_rows[0]
    assert len(actual_rows) == 1
    assert actual_row[land_url_idx] == LAND_URL_B
    assert actual_row[thm_idx] == THM_URL_A
    assert actual_row[filesize_idx] == FILESIZE
    assert actual_row[license_idx] == LICENSE_B
    assert actual_row[version_idx] == VERSION_B
    assert actual_row[creator_idx] == CREATOR_A
    assert actual_row[creator_url_idx] == CREATOR_URL_A
    assert actual_row[title_idx] == TITLE_A
    assert actual_row[metadata_idx] == json.loads(META_DATA_A)
    assert actual_row[tags_idx] == json.loads(TAGS)
    assert actual_row[width_idx] == WIDTH_A
    assert actual_row[height_idx] == HEIGHT_A


def test_upsert_records_merges_meta_data(postgres_with_load_and_image_table, tmpdir):
    postgres_conn_id = POSTGRES_CONN_ID
    load_table = TEST_LOAD_TABLE
    image_table = TEST_IMAGE_TABLE
    identifier = TEST_ID

    FID = "a"
    PROVIDER = "images_provider"
    IMG_URL = "https://images.com/a/img.jpg"
    LICENSE = "by"

    META_DATA_A = '{"description": "a cool picture", "test": "should stay"}'
    META_DATA_B = '{"description": "I updated my description"}'

    query_values_a = create_query_values(
        {
            col.FOREIGN_ID: FID,
            col.DIRECT_URL: IMG_URL,
            col.LICENSE: LICENSE,
            col.META_DATA: META_DATA_A,
            col.PROVIDER: PROVIDER,
        }
    )
    load_data_query_a = f"""INSERT INTO {load_table} VALUES(
        {query_values_a}
    );"""

    query_values_b = create_query_values(
        {
            col.FOREIGN_ID: FID,
            col.DIRECT_URL: IMG_URL,
            col.LICENSE: LICENSE,
            col.META_DATA: META_DATA_B,
            col.PROVIDER: PROVIDER,
        }
    )
    load_data_query_b = f"""INSERT INTO {load_table} VALUES(
        {query_values_b}
        );"""
    postgres_with_load_and_image_table.cursor.execute(load_data_query_a)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"DELETE FROM {load_table};")
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(load_data_query_b)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"SELECT * FROM {image_table};")
    actual_rows = postgres_with_load_and_image_table.cursor.fetchall()
    actual_row = actual_rows[0]
    assert len(actual_rows) == 1
    expected_meta_data = json.loads(META_DATA_A)
    expected_meta_data.update(json.loads(META_DATA_B))
    assert actual_row[metadata_idx] == expected_meta_data


def test_upsert_records_does_not_replace_with_null_values_in_meta_data(
    postgres_with_load_and_image_table, tmpdir
):
    postgres_conn_id = POSTGRES_CONN_ID
    load_table = TEST_LOAD_TABLE
    image_table = TEST_IMAGE_TABLE
    identifier = TEST_ID

    FID = "a"
    PROVIDER = "images_provider"
    IMG_URL = "https://images.com/a/img.jpg"
    LICENSE = "by"

    META_DATA_A = '{"description": "a cool picture", "test": "should stay"}'
    META_DATA_B = '{"description": "I updated my description", "test": null}'

    query_values_a = create_query_values(
        {
            col.FOREIGN_ID: FID,
            col.DIRECT_URL: IMG_URL,
            col.LICENSE: LICENSE,
            col.META_DATA: META_DATA_A,
            col.PROVIDER: PROVIDER,
        }
    )
    load_data_query_a = f"""INSERT INTO {load_table} VALUES(
        {query_values_a}
        );"""

    query_values_b = create_query_values(
        {
            col.FOREIGN_ID: FID,
            col.DIRECT_URL: IMG_URL,
            col.LICENSE: LICENSE,
            col.META_DATA: META_DATA_B,
            col.PROVIDER: PROVIDER,
        }
    )
    load_data_query_b = f"""INSERT INTO {load_table} VALUES(
        {query_values_b}
        );"""
    postgres_with_load_and_image_table.cursor.execute(load_data_query_a)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"DELETE FROM {load_table};")
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(load_data_query_b)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"SELECT * FROM {image_table};")
    actual_rows = postgres_with_load_and_image_table.cursor.fetchall()
    actual_row = actual_rows[0]
    assert len(actual_rows) == 1
    expected_meta_data = {
        "description": json.loads(META_DATA_B)["description"],
        "test": json.loads(META_DATA_A)["test"],
    }
    assert actual_row[metadata_idx] == expected_meta_data


def test_upsert_records_merges_tags(postgres_with_load_and_image_table, tmpdir):
    postgres_conn_id = POSTGRES_CONN_ID
    load_table = TEST_LOAD_TABLE
    image_table = TEST_IMAGE_TABLE
    identifier = TEST_ID

    FID = "a"
    PROVIDER = "images_provider"
    IMG_URL = "https://images.com/a/img.jpg"
    LICENSE = "by"

    TAGS_A = json.dumps(
        [{"name": "tagone", "provider": "test"}, {"name": "tagtwo", "provider": "test"}]
    )
    TAGS_B = json.dumps(
        [
            {"name": "tagone", "provider": "test"},
            {"name": "tagthree", "provider": "test"},
        ]
    )

    query_values_a = create_query_values(
        {
            col.FOREIGN_ID: FID,
            col.DIRECT_URL: IMG_URL,
            col.LICENSE: LICENSE,
            col.TAGS: TAGS_A,
            col.PROVIDER: PROVIDER,
        }
    )
    load_data_query_a = f"""INSERT INTO {load_table} VALUES(
        {query_values_a}
        );"""

    query_values_b = create_query_values(
        {
            col.FOREIGN_ID: FID,
            col.DIRECT_URL: IMG_URL,
            col.LICENSE: LICENSE,
            col.TAGS: TAGS_B,
            col.PROVIDER: PROVIDER,
        }
    )
    load_data_query_b = f"""INSERT INTO {load_table} VALUES(
        {query_values_b}
        );"""
    postgres_with_load_and_image_table.cursor.execute(load_data_query_a)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"DELETE FROM {load_table};")
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(load_data_query_b)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"SELECT * FROM {image_table};")
    actual_rows = postgres_with_load_and_image_table.cursor.fetchall()
    actual_row = actual_rows[0]
    assert len(actual_rows) == 1
    expect_tags = [
        {"name": "tagone", "provider": "test"},
        {"name": "tagtwo", "provider": "test"},
        {"name": "tagthree", "provider": "test"},
    ]
    actual_tags = actual_row[tags_idx]
    assert len(actual_tags) == 3
    assert all([t in expect_tags for t in actual_tags])
    assert all([t in actual_tags for t in expect_tags])


def test_upsert_records_does_not_replace_tags_with_null(
    postgres_with_load_and_image_table, tmpdir
):
    postgres_conn_id = POSTGRES_CONN_ID
    load_table = TEST_LOAD_TABLE
    image_table = TEST_IMAGE_TABLE
    identifier = TEST_ID

    FID = "a"
    PROVIDER = "images_provider"
    IMG_URL = "https://images.com/a/img.jpg"
    LICENSE = "by"

    TAGS = [
        {"name": "tagone", "provider": "test"},
        {"name": "tagtwo", "provider": "test"},
    ]

    query_values_a = create_query_values(
        {
            col.FOREIGN_ID: FID,
            col.DIRECT_URL: IMG_URL,
            col.LICENSE: LICENSE,
            col.TAGS: json.dumps(TAGS),
            col.PROVIDER: PROVIDER,
        }
    )
    load_data_query_a = f"""INSERT INTO {load_table} VALUES(
        {query_values_a}
        );"""

    query_values_b = create_query_values(
        {
            col.FOREIGN_ID: FID,
            col.DIRECT_URL: IMG_URL,
            col.LICENSE: LICENSE,
            col.PROVIDER: PROVIDER,
        }
    )
    load_data_query_b = f"""INSERT INTO {load_table} VALUES(
        {query_values_b}
        );"""
    postgres_with_load_and_image_table.cursor.execute(load_data_query_a)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"DELETE FROM {load_table};")
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(load_data_query_b)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"SELECT * FROM {image_table};")
    actual_rows = postgres_with_load_and_image_table.cursor.fetchall()
    actual_row = actual_rows[0]
    assert len(actual_rows) == 1
    expect_tags = [
        {"name": "tagone", "provider": "test"},
        {"name": "tagtwo", "provider": "test"},
    ]
    actual_tags = actual_row[tags_idx]
    assert len(actual_tags) == 2
    assert all([t in expect_tags for t in actual_tags])
    assert all([t in actual_tags for t in expect_tags])


def test_upsert_records_replaces_null_tags(postgres_with_load_and_image_table, tmpdir):
    postgres_conn_id = POSTGRES_CONN_ID
    load_table = TEST_LOAD_TABLE
    image_table = TEST_IMAGE_TABLE
    identifier = TEST_ID

    FID = "a"
    PROVIDER = "images_provider"
    IMG_URL = "https://images.com/a/img.jpg"
    LICENSE = "by"
    TAGS = [
        {"name": "tagone", "provider": "test"},
        {"name": "tagtwo", "provider": "test"},
    ]
    query_values_a = create_query_values(
        {
            col.FOREIGN_ID: FID,
            col.DIRECT_URL: IMG_URL,
            col.LICENSE: LICENSE,
            col.PROVIDER: PROVIDER,
        }
    )
    logging.info(f"Query values a: {query_values_a}")
    load_data_query_a = f"""INSERT INTO {load_table} VALUES(
        {query_values_a}
        );"""
    query_values_b = create_query_values(
        {
            col.FOREIGN_ID: FID,
            col.DIRECT_URL: IMG_URL,
            col.LICENSE: LICENSE,
            col.TAGS: json.dumps(TAGS),
            col.PROVIDER: PROVIDER,
        }
    )
    load_data_query_b = f"""INSERT INTO {load_table} VALUES(
        {query_values_b}
        );"""

    postgres_with_load_and_image_table.cursor.execute(load_data_query_a)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"DELETE FROM {load_table};")
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(load_data_query_b)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"SELECT * FROM {image_table};")
    actual_rows = postgres_with_load_and_image_table.cursor.fetchall()
    actual_row = actual_rows[0]
    logging.info(f"Actual row: {actual_row}")
    assert len(actual_rows) == 1
    expect_tags = [
        {"name": "tagone", "provider": "test"},
        {"name": "tagtwo", "provider": "test"},
    ]
    actual_tags = actual_row[tags_idx]
    assert len(actual_tags) == 2
    assert all([t in expect_tags for t in actual_tags])
    assert all([t in actual_tags for t in expect_tags])


def test_overwrite_records_leaves_dates(postgres_with_load_and_image_table, tmpdir):
    postgres_conn_id = POSTGRES_CONN_ID
    load_table = TEST_LOAD_TABLE
    image_table = TEST_IMAGE_TABLE
    identifier = TEST_ID

    FID = "a"
    LAND_URL = "https://images.com/a"
    IMG_URL = "images.com/a/img.jpg"
    LICENSE = "cc0"
    VERSION = "1.0"
    PROVIDER = "images"

    load_data_query = f"""INSERT INTO {load_table} (
        foreign_identifier, foreign_landing_url, url,
         license, license_version, provider, source
        ) VALUES (
        '{FID}','{LAND_URL}','{IMG_URL}',
        '{LICENSE}','{VERSION}','{PROVIDER}', '{PROVIDER}'
        );"""
    postgres_with_load_and_image_table.cursor.execute(load_data_query)
    postgres_with_load_and_image_table.connection.commit()

    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.cursor.execute(f"SELECT * FROM {image_table};")
    original_row = postgres_with_load_and_image_table.cursor.fetchall()[0]
    original_updated_on = original_row[updated_idx]
    original_last_synced = original_row[synced_idx]

    time.sleep(0.001)
    sql.overwrite_records_in_db_table(
        postgres_conn_id, identifier, db_table=image_table
    )
    postgres_with_load_and_image_table.cursor.execute(f"SELECT * FROM {image_table};")
    updated_result = postgres_with_load_and_image_table.cursor.fetchall()
    updated_row = updated_result[0]
    updated_updated_on = updated_row[updated_idx]
    updated_last_synced = updated_row[synced_idx]

    assert len(updated_result) == 1
    assert updated_updated_on == original_updated_on
    assert updated_last_synced == original_last_synced


def test_overwrite_records_replaces_data(postgres_with_load_and_image_table, tmpdir):
    postgres_conn_id = POSTGRES_CONN_ID
    load_table = TEST_LOAD_TABLE
    image_table = TEST_IMAGE_TABLE
    identifier = TEST_ID

    FID = "a"
    PROVIDER = "images_provider"
    SOURCE = "images_source"
    WATERMARKED = "f"
    FILESIZE = 2000
    TAGS = '["fun", "great"]'

    IMG_URL_A = "https://images.com/a/img.jpg"
    LAND_URL_A = "https://images.com/a"
    THM_URL_A = "https://images.com/a/img_small.jpg"
    WIDTH_A = 1000
    HEIGHT_A = 500
    LICENSE_A = "by"
    VERSION_A = "4.0"
    CREATOR_A = "Alice"
    CREATOR_URL_A = "https://alice.com"
    TITLE_A = "My Great Pic"
    META_DATA_A = '{"description": "what a cool picture"}'

    IMG_URL_B = "https://images.com/b/img.jpg"
    LAND_URL_B = "https://images.com/b"
    THM_URL_B = "https://images.com/b/img_small.jpg"
    WIDTH_B = 2000
    HEIGHT_B = 1000
    LICENSE_B = "cc0"
    VERSION_B = "1.0"
    CREATOR_B = "Bob"
    CREATOR_URL_B = "https://bob.com"
    TITLE_B = "Bobs Great Pic"
    META_DATA_B = '{"mydesc": "Bobs cool picture"}'

    query_values_a = create_query_values(
        {
            col.FOREIGN_ID: FID,
            col.LANDING_URL: LAND_URL_A,
            col.DIRECT_URL: IMG_URL_A,
            col.THUMBNAIL: THM_URL_A,
            col.FILESIZE: FILESIZE,
            col.LICENSE: LICENSE_A,
            col.LICENSE_VERSION: VERSION_A,
            col.CREATOR: CREATOR_A,
            col.CREATOR_URL: CREATOR_URL_A,
            col.TITLE: TITLE_A,
            col.META_DATA: META_DATA_A,
            col.TAGS: TAGS,
            col.WATERMARKED: WATERMARKED,
            col.PROVIDER: PROVIDER,
            col.SOURCE: SOURCE,
            col.WIDTH: WIDTH_A,
            col.HEIGHT: HEIGHT_A,
        }
    )
    load_data_query_a = f"""INSERT INTO {load_table} VALUES(
        {query_values_a}
        );"""

    postgres_with_load_and_image_table.cursor.execute(load_data_query_a)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()

    query_values_b = create_query_values(
        {
            col.FOREIGN_ID: FID,
            col.LANDING_URL: LAND_URL_B,
            col.DIRECT_URL: IMG_URL_B,
            col.THUMBNAIL: THM_URL_B,
            col.FILESIZE: FILESIZE,
            col.LICENSE: LICENSE_B,
            col.LICENSE_VERSION: VERSION_B,
            col.CREATOR: CREATOR_B,
            col.CREATOR_URL: CREATOR_URL_B,
            col.TITLE: TITLE_B,
            col.META_DATA: META_DATA_B,
            col.TAGS: TAGS,
            col.WATERMARKED: WATERMARKED,
            col.PROVIDER: PROVIDER,
            col.SOURCE: SOURCE,
            col.WIDTH: WIDTH_B,
            col.HEIGHT: HEIGHT_B,
        }
    )
    load_data_query_b = f"""INSERT INTO {load_table} VALUES(
        {query_values_b}
        );"""
    postgres_with_load_and_image_table.cursor.execute(f"DELETE FROM {load_table};")
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(load_data_query_b)
    postgres_with_load_and_image_table.connection.commit()
    sql.overwrite_records_in_db_table(
        postgres_conn_id, identifier, db_table=image_table
    )
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"SELECT * FROM {image_table};")
    actual_rows = postgres_with_load_and_image_table.cursor.fetchall()
    actual_row = actual_rows[0]
    assert len(actual_rows) == 1
    assert actual_row[land_url_idx] == LAND_URL_B
    assert actual_row[url_idx] == IMG_URL_B
    assert actual_row[thm_idx] == THM_URL_B
    assert actual_row[license_idx] == LICENSE_B
    assert actual_row[version_idx] == VERSION_B
    assert actual_row[creator_idx] == CREATOR_B
    assert actual_row[creator_url_idx] == CREATOR_URL_B
    assert actual_row[title_idx] == TITLE_B
    assert actual_row[metadata_idx] == json.loads(META_DATA_B)
    assert actual_row[width_idx] == WIDTH_B
    assert actual_row[height_idx] == HEIGHT_B


def test_drop_load_table_drops_table(postgres_with_load_table):
    postgres_conn_id = POSTGRES_CONN_ID
    identifier = TEST_ID
    load_table = TEST_LOAD_TABLE
    sql.drop_load_table(postgres_conn_id, identifier, ti)
    check_query = (
        f"SELECT EXISTS (" f"SELECT FROM pg_tables WHERE tablename='{load_table}');"
    )
    postgres_with_load_table.cursor.execute(check_query)
    check_result = postgres_with_load_table.cursor.fetchone()[0]
    assert not check_result


def test_update_flickr_sub_providers(postgres_with_load_and_image_table):
    postgres_conn_id = POSTGRES_CONN_ID
    load_table = TEST_LOAD_TABLE
    image_table = TEST_IMAGE_TABLE
    identifier = TEST_ID

    FID_A = "a"
    FID_B = "b"
    IMG_URL_A = "https://images.com/a/img.jpg"
    IMG_URL_B = "https://images.com/b/img.jpg"
    CREATOR_URL_A = "https://www.flickr.com/photos/29988733@N04"
    CREATOR_URL_B = "https://www.flickr.com/photos/other_user"
    PROVIDER = "flickr"
    LICENSE = "by"
    TAGS = [
        {"name": "tagone", "provider": "test"},
        {"name": "tagtwo", "provider": "test"},
    ]

    insert_data_query = f"""INSERT INTO {load_table} VALUES
        ({create_query_values({
        col.FOREIGN_ID: FID_A,
        col.DIRECT_URL: IMG_URL_A,
        col.LICENSE: LICENSE,
        col.CREATOR_URL: CREATOR_URL_A,
        col.TAGS: json.dumps(TAGS),
        col.PROVIDER: PROVIDER,
        col.SOURCE: PROVIDER,
        })}),
        ({create_query_values({
        col.FOREIGN_ID: FID_B,
        col.DIRECT_URL: IMG_URL_B,
        col.LICENSE: LICENSE,
        col.CREATOR_URL: CREATOR_URL_B,
        col.TAGS: json.dumps(TAGS),
        col.PROVIDER: PROVIDER,
        col.SOURCE: PROVIDER,
        })});"""

    postgres_with_load_and_image_table.cursor.execute(insert_data_query)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"DELETE FROM {load_table};")
    postgres_with_load_and_image_table.connection.commit()

    sql.update_flickr_sub_providers(postgres_conn_id, image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"SELECT * FROM {image_table};")
    actual_rows = postgres_with_load_and_image_table.cursor.fetchall()
    assert len(actual_rows) == 2

    for actual_row in actual_rows:
        if actual_row[fid_idx] == "a":
            assert actual_row[source_idx] == "nasa"
        else:
            assert actual_row[fid_idx] == "b" and actual_row[source_idx] == "flickr"


def test_update_europeana_sub_providers(postgres_with_load_and_image_table):
    postgres_conn_id = POSTGRES_CONN_ID
    load_table = TEST_LOAD_TABLE
    image_table = TEST_IMAGE_TABLE
    identifier = TEST_ID

    FID_A = "a"
    FID_B = "b"
    IMG_URL_A = "https://images.com/a/img.jpg"
    IMG_URL_B = "https://images.com/b/img.jpg"
    PROVIDER = "europeana"
    LICENSE = "by-nc-nd"
    META_DATA_A = {
        "country": ["Sweden"],
        "dataProvider": ["Wellcome Collection"],
        "description": "A",
        "license_url": "http://creativecommons.org/licenses/by-nc-nd/4.0/",
    }
    META_DATA_B = {
        "country": ["Sweden"],
        "dataProvider": ["Other Collection"],
        "description": "B",
        "license_url": "http://creativecommons.org/licenses/by-nc-nd/4.0/",
    }

    query_values = [
        create_query_values(
            {
                col.FOREIGN_ID: FID_A,
                col.DIRECT_URL: IMG_URL_A,
                col.LICENSE: LICENSE,
                col.META_DATA: json.dumps(META_DATA_A),
                col.PROVIDER: PROVIDER,
                col.SOURCE: PROVIDER,
            }
        ),
        create_query_values(
            {
                col.FOREIGN_ID: FID_B,
                col.DIRECT_URL: IMG_URL_B,
                col.LICENSE: LICENSE,
                col.META_DATA: json.dumps(META_DATA_B),
                col.PROVIDER: PROVIDER,
                col.SOURCE: PROVIDER,
            }
        ),
    ]
    insert_data_query = f"""INSERT INTO {load_table} VALUES
        ({query_values[0]}),
        ({query_values[1]}
        );"""

    postgres_with_load_and_image_table.cursor.execute(insert_data_query)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"DELETE FROM {load_table};")
    postgres_with_load_and_image_table.connection.commit()

    sql.update_europeana_sub_providers(postgres_conn_id, image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"SELECT * FROM {image_table};")
    actual_rows = postgres_with_load_and_image_table.cursor.fetchall()
    assert len(actual_rows) == 2

    for actual_row in actual_rows:
        if actual_row[fid_idx] == "a":
            assert actual_row[source_idx] == "wellcome_collection"
        else:
            assert actual_row[fid_idx] == "b" and actual_row[source_idx] == "europeana"


def test_update_smithsonian_sub_providers(postgres_with_load_and_image_table):
    postgres_conn_id = POSTGRES_CONN_ID
    load_table = TEST_LOAD_TABLE
    image_table = TEST_IMAGE_TABLE
    identifier = TEST_ID

    FID_A = "a"
    FID_B = "b"
    IMG_URL_A = "https://images.com/a/img.jpg"
    IMG_URL_B = "https://images.com/b/img.jpg"
    PROVIDER = "smithsonian"
    LICENSE = "by-nc-nd"
    META_DATA_A = {
        "unit_code": "SIA",
        "data_source": "Smithsonian Institution Archives",
    }
    META_DATA_B = {
        "unit_code": "NMNHBIRDS",
        "data_source": "NMNH - Vertebrate Zoology - Birds Division",
    }

    query_values = [
        create_query_values(
            {
                col.FOREIGN_ID: FID_A,
                col.DIRECT_URL: IMG_URL_A,
                col.LICENSE: LICENSE,
                col.META_DATA: json.dumps(META_DATA_A),
                col.PROVIDER: PROVIDER,
                col.SOURCE: PROVIDER,
            }
        ),
        create_query_values(
            {
                col.FOREIGN_ID: FID_B,
                col.DIRECT_URL: IMG_URL_B,
                col.LICENSE: LICENSE,
                col.META_DATA: json.dumps(META_DATA_B),
                col.PROVIDER: PROVIDER,
                col.SOURCE: PROVIDER,
            }
        ),
    ]
    insert_data_query = f"""INSERT INTO {load_table} VALUES
        ({query_values[0]}),
        ({query_values[1]}
        );"""

    postgres_with_load_and_image_table.cursor.execute(insert_data_query)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"DELETE FROM {load_table};")
    postgres_with_load_and_image_table.connection.commit()

    sql.update_smithsonian_sub_providers(postgres_conn_id, image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"SELECT * FROM {image_table};")
    actual_rows = postgres_with_load_and_image_table.cursor.fetchall()
    assert len(actual_rows) == 2

    for actual_row in actual_rows:
        if actual_row[fid_idx] == "a":
            assert actual_row[source_idx] == "smithsonian_institution_archives"
        else:
            assert (
                actual_row[fid_idx] == "b"
                and actual_row[source_idx]
                == "smithsonian_national_museum_of_natural_history"
            )


def test_image_expiration(postgres_with_load_and_image_table):
    postgres_conn_id = POSTGRES_CONN_ID
    load_table = TEST_LOAD_TABLE
    image_table = TEST_IMAGE_TABLE
    identifier = TEST_ID

    FID_A = "a"
    FID_B = "b"
    IMG_URL_A = "https://images.com/a/img.jpg"
    IMG_URL_B = "https://images.com/b/img.jpg"
    PROVIDER_A = "smithsonian"
    PROVIDER_B = "flickr"
    LICENSE = "by-nc-nd"

    query_values = [
        create_query_values(
            {
                col.FOREIGN_ID: FID_A,
                col.DIRECT_URL: IMG_URL_A,
                col.LICENSE: LICENSE,
                col.PROVIDER: PROVIDER_A,
                col.SOURCE: PROVIDER_A,
            }
        ),
        create_query_values(
            {
                col.FOREIGN_ID: FID_B,
                col.DIRECT_URL: IMG_URL_B,
                col.LICENSE: LICENSE,
                col.PROVIDER: PROVIDER_B,
                col.SOURCE: PROVIDER_B,
            }
        ),
    ]
    insert_data_query = f"""INSERT INTO {load_table} VALUES
        ({query_values[0]}),
        ({query_values[1]}
        );"""

    postgres_with_load_and_image_table.cursor.execute(insert_data_query)
    postgres_with_load_and_image_table.connection.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    postgres_with_load_and_image_table.connection.commit()
    postgres_with_load_and_image_table.cursor.execute(f"DELETE FROM {load_table};")
    postgres_with_load_and_image_table.connection.commit()

    postgres_with_load_and_image_table.cursor.execute(
        f"UPDATE {image_table} SET updated_on = NOW() - INTERVAL '1 year' "
        f"WHERE provider = 'flickr';"
    )

    postgres_with_load_and_image_table.connection.commit()

    sql.expire_old_images(postgres_conn_id, PROVIDER_A, image_table=image_table)

    sql.expire_old_images(postgres_conn_id, PROVIDER_B, image_table=image_table)

    postgres_with_load_and_image_table.connection.commit()

    postgres_with_load_and_image_table.cursor.execute(f"SELECT * FROM {image_table};")
    actual_rows = postgres_with_load_and_image_table.cursor.fetchall()
    assert len(actual_rows) == 2

    for actual_row in actual_rows:
        if actual_row[fid_idx] == "a":
            assert not actual_row[removed_idx]
        else:
            assert actual_row[fid_idx] == "b" and actual_row[removed_idx]

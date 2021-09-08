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
from util.loader import sql


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

CREATE_LOAD_TABLE_QUERY = (
    f"CREATE TABLE public.{TEST_LOAD_TABLE} ("
    f"foreign_identifier character varying(3000), "
    f"foreign_landing_url character varying(1000), "
    f"url character varying(3000), "
    f"thumbnail character varying(3000), "
    f"filesize integer, "
    f"license character varying(50), "
    f"license_version character varying(25), "
    f"creator character varying(2000), "
    f"creator_url character varying(2000), "
    f"title character varying(5000), "
    f"meta_data jsonb, "
    f"tags jsonb, "
    f"watermarked boolean, "
    f"provider character varying(80), "
    f"source character varying(80), "
    f"ingestion_type character varying(80), "
    f"width integer, "
    f"height integer"
    f");"
)

UUID_FUNCTION_QUERY = 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;'

CREATE_IMAGE_TABLE_QUERY = (
    f"CREATE TABLE public.{TEST_IMAGE_TABLE} ("
    "identifier uuid PRIMARY KEY DEFAULT public.uuid_generate_v4(),"  # 0
    "created_on timestamp with time zone NOT NULL,"  # 1
    "updated_on timestamp with time zone NOT NULL,"  # 2
    "ingestion_type character varying(80),"  # 3
    "provider character varying(80),"  # 4
    "source character varying(80),"  # 5
    "foreign_identifier character varying(3000),"  # 6
    "foreign_landing_url character varying(1000),"  # 7
    "url character varying(3000) NOT NULL,"  # 8
    "thumbnail character varying(3000),"  # 9
    "filesize integer,"  # 10
    "license character varying(50) NOT NULL,"  # 11
    "license_version character varying(25),"  # 12
    "creator character varying(2000),"  # 13
    "creator_url character varying(2000),"  # 14
    "title character varying(5000),"  # 15
    "meta_data jsonb,"  # 16
    "tags jsonb,"  # 17
    "watermarked boolean,"  # 18
    "last_synced_with_source timestamp with time zone,"  # 19
    "removed_from_source boolean NOT NULL,"  # 20
    "width integer,"  # 21
    "height integer"  # 22
    f");"
)

UNIQUE_CONDITION_QUERY = (
    f"CREATE UNIQUE INDEX {TEST_IMAGE_TABLE}_provider_fid_idx"
    f" ON public.{TEST_IMAGE_TABLE}"
    " USING btree (provider, md5(foreign_identifier));"
)

DROP_IMAGE_INDEX_QUERY = f"DROP INDEX IF EXISTS {TEST_IMAGE_TABLE}_provider_fid_idx;"

op_no_dag = DummyOperator(task_id="op_no_dag")
naive_datetime = datetime(2016, 1, 1).replace(tzinfo=None)
ti = TaskInstance(task=op_no_dag, execution_date=naive_datetime)


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
    # logging.info(f'Created load table with {create_command}')
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

    # logging.info(f"dropping load table: {DROP_LOAD_TABLE_QUERY}")
    # logging.info(f"dropping image table: {DROP_IMAGE_TABLE_QUERY}")
    cur.execute(DROP_LOAD_TABLE_QUERY)
    cur.execute(DROP_IMAGE_TABLE_QUERY)
    cur.execute(DROP_IMAGE_INDEX_QUERY)
    cur.execute(CREATE_LOAD_TABLE_QUERY)
    cur.execute(UUID_FUNCTION_QUERY)
    cur.execute(CREATE_IMAGE_TABLE_QUERY)
    cur.execute(UNIQUE_CONDITION_QUERY)
    # logging.info(f'Created load table with {CREATE_LOAD_TABLE_QUERY}')
    # logging.info(f'Created image table with {CREATE_IMAGE_TABLE_QUERY}')

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

    load_data_query = (
        f"INSERT INTO {load_table} VALUES("
        f"'{FID}','{LAND_URL}','{IMG_URL}','{THM_URL}',"
        f"'{FILESIZE}','{LICENSE}','{VERSION}','{CREATOR}','{CREATOR_URL}',"
        f"'{TITLE}','{META_DATA}','{TAGS}','{WATERMARKED}','{PROVIDER}',"
        f"'{SOURCE}', '{INGESTION_TYPE}', '{WIDTH}', '{HEIGHT}');"
    )
    c = postgres_with_load_and_image_table.cursor
    conn = postgres_with_load_and_image_table.connection
    c.execute(load_data_query)
    conn.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    c.execute(f"SELECT * FROM {image_table};")
    actual_rows = c.fetchall()
    actual_row = actual_rows[0]
    assert len(actual_rows) == 1
    assert actual_row[3] == INGESTION_TYPE
    assert actual_row[4] == PROVIDER
    assert actual_row[5] == SOURCE
    assert actual_row[6] == FID
    assert actual_row[7] == LAND_URL
    assert actual_row[8] == IMG_URL
    assert actual_row[9] == THM_URL
    assert actual_row[10] == FILESIZE
    assert actual_row[11] == LICENSE
    assert actual_row[12] == VERSION
    assert actual_row[13] == CREATOR
    assert actual_row[14] == CREATOR_URL
    assert actual_row[15] == TITLE
    assert actual_row[16] == json.loads(META_DATA)
    assert actual_row[17] == json.loads(TAGS)
    assert actual_row[18] is False
    assert actual_row[21] == WIDTH
    assert actual_row[22] == HEIGHT


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

    c = postgres_with_load_and_image_table.cursor
    conn = postgres_with_load_and_image_table.connection

    for r in test_rows:
        load_data_query = (
            f"INSERT INTO {load_table} ("
            f"foreign_identifier, foreign_landing_url, url,"
            f" license, license_version, provider, source"
            f") VALUES ("
            f"'{r[0]}', '{r[1]}', '{r[2]}',"
            f"'{r[3]}', '{r[4]}', '{r[5]}', '{r[5]}'"
            f");"
        )
        c.execute(load_data_query)
        conn.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    c.execute(f"SELECT * FROM {image_table};")
    actual_rows = c.fetchall()
    assert actual_rows[0][6] == FID_A
    assert actual_rows[1][6] == FID_B


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

    load_data_query = (
        f"INSERT INTO {load_table} ("
        f"foreign_identifier, foreign_landing_url, url,"
        f" license, license_version, provider, source"
        f") VALUES ("
        f"'{FID}','{LAND_URL}','{IMG_URL}',"
        f"'{LICENSE}','{VERSION}','{PROVIDER}','{PROVIDER}'"
        f");"
    )
    c = postgres_with_load_and_image_table.cursor
    conn = postgres_with_load_and_image_table.connection
    c.execute(load_data_query)
    conn.commit()

    updated_on_idx = 2
    last_synced_idx = -4

    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    c.execute(f"SELECT * FROM {image_table};")
    original_row = c.fetchall()[0]
    logging.info(f"***\nORIGINAL ROW\n{original_row}")
    original_updated_on = original_row[updated_on_idx]
    original_last_synced = original_row[last_synced_idx]

    time.sleep(0.001)
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    c.execute(f"SELECT * FROM {image_table};")
    updated_result = c.fetchall()
    updated_row = updated_result[0]
    updated_updated_on = updated_row[updated_on_idx]
    updated_last_synced = updated_row[last_synced_idx]

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
    INGESTION = "provider API"

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
    c = postgres_with_load_and_image_table.cursor
    conn = postgres_with_load_and_image_table.connection

    load_data_query_a = (
        f"INSERT INTO {load_table} VALUES("
        f"'{FID}','{LAND_URL_A}','{IMG_URL_A}','{THM_URL_A}',"
        f"'{FILESIZE}','{LICENSE_A}','{VERSION_A}',"
        f"'{CREATOR_A}','{CREATOR_URL_A}','{TITLE_A}','{META_DATA_A}',"
        f"'{TAGS}','{WATERMARKED}','{PROVIDER}','{SOURCE}','{INGESTION}',"
        f"'{WIDTH_A}','{HEIGHT_A}'"
        f");"
    )
    c.execute(load_data_query_a)
    conn.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    conn.commit()

    load_data_query_b = (
        f"INSERT INTO {load_table} VALUES("
        f"'{FID}','{LAND_URL_B}','{IMG_URL_B}','{THM_URL_B}',"
        f"'{FILESIZE}','{LICENSE_B}','{VERSION_B}',"
        f"'{CREATOR_B}','{CREATOR_URL_B}','{TITLE_B}','{META_DATA_B}',"
        f"'{TAGS}','{WATERMARKED}','{PROVIDER}','{SOURCE}','{INGESTION}',"
        f"'{WIDTH_B}','{HEIGHT_B}'"
        f");"
    )
    c.execute(f"DELETE FROM {load_table};")
    conn.commit()
    c.execute(load_data_query_b)
    conn.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    conn.commit()
    c.execute(f"SELECT * FROM {image_table};")
    actual_rows = c.fetchall()
    actual_row = actual_rows[0]
    assert len(actual_rows) == 1
    assert actual_row[7] == LAND_URL_B
    assert actual_row[8] == IMG_URL_B
    assert actual_row[9] == THM_URL_B
    assert actual_row[10] == WIDTH_B
    assert actual_row[11] == LICENSE_B
    assert actual_row[12] == VERSION_B
    assert actual_row[13] == CREATOR_B
    assert actual_row[14] == CREATOR_URL_B
    assert actual_row[15] == TITLE_B
    assert actual_row[16] == json.loads(META_DATA_B)
    assert actual_row[17] == json.loads(TAGS)


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
    INGESTION = "provider API"

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

    load_data_query_a = (
        f"INSERT INTO {load_table} VALUES("
        f"'{FID}','{LAND_URL_A}','{IMG_URL}','{THM_URL_A}',"
        f"'{FILESIZE}','{LICENSE_A}','{VERSION_A}',"
        f"'{CREATOR_A}','{CREATOR_URL_A}','{TITLE_A}','{META_DATA_A}',"
        f"'{TAGS}','{WATERMARKED}','{PROVIDER}','{SOURCE}','{INGESTION}',"
        f"'{WIDTH_A}','{HEIGHT_A}'"
        f");"
    )
    c = postgres_with_load_and_image_table.cursor
    conn = postgres_with_load_and_image_table.connection
    c.execute(load_data_query_a)
    conn.commit()
    c.execute(f"SELECT * FROM {load_table};")
    upsert_result = c.fetchall()[0]
    logging.info(f"UPSERT A:\n{upsert_result}")
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    conn.commit()
    c.execute(f"SELECT * FROM {image_table};")
    upsert_result = c.fetchall()[0]
    logging.info(f"IMAGE after A:\n{upsert_result}")
    load_data_query_b = (
        f"INSERT INTO {load_table} VALUES("
        f"'{FID}','{LAND_URL_B}','{IMG_URL}',null,"
        f"null,'{LICENSE_B}','{VERSION_B}',"
        f"null,null,null,null,"
        f"'{TAGS}',null,'{PROVIDER}','{SOURCE}',null,null,null"
        f");"
    )
    c.execute(f"DELETE FROM {load_table};")
    conn.commit()
    c.execute(load_data_query_b)
    conn.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    conn.commit()
    c.execute(f"SELECT * FROM {load_table};")
    upsert_result = c.fetchall()[0]
    logging.info(f"UPSERT B:\n{upsert_result}")
    c.execute(f"SELECT * FROM {image_table};")
    actual_rows = c.fetchall()
    actual_row = actual_rows[0]
    logging.info(f"***ACTUAL ROW:\n{actual_row}")
    assert len(actual_rows) == 1
    assert actual_row[7] == LAND_URL_B
    assert actual_row[9] == THM_URL_A
    assert actual_row[10] == FILESIZE
    assert actual_row[11] == LICENSE_B
    assert actual_row[12] == VERSION_B
    assert actual_row[13] == CREATOR_A
    assert actual_row[14] == CREATOR_URL_A
    assert actual_row[15] == TITLE_A
    assert actual_row[16] == json.loads(META_DATA_A)
    assert actual_row[17] == json.loads(TAGS)
    assert actual_row[21] == WIDTH_A
    assert actual_row[22] == HEIGHT_A


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

    load_data_query_a = (
        f"INSERT INTO {load_table} VALUES("
        f"'{FID}',null,'{IMG_URL}',null,null,'{LICENSE}',null,null,"
        f"null,null,'{META_DATA_A}',null,null,'{PROVIDER}',null,null,null,null"
        f");"
    )

    load_data_query_b = (
        f"INSERT INTO {load_table} VALUES("
        f"'{FID}',null,'{IMG_URL}',null,null,'{LICENSE}',null,null,"
        f"null,null,'{META_DATA_B}',null,null,'{PROVIDER}',null,null,null,null"
        f");"
    )
    c = postgres_with_load_and_image_table.cursor
    conn = postgres_with_load_and_image_table.connection
    c.execute(load_data_query_a)
    conn.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    conn.commit()
    c.execute(f"DELETE FROM {load_table};")
    conn.commit()
    c.execute(load_data_query_b)
    conn.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    conn.commit()
    c.execute(f"SELECT * FROM {image_table};")
    actual_rows = c.fetchall()
    actual_row = actual_rows[0]
    assert len(actual_rows) == 1
    expected_meta_data = json.loads(META_DATA_A)
    expected_meta_data.update(json.loads(META_DATA_B))
    assert actual_row[16] == expected_meta_data


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

    load_data_query_a = (
        f"INSERT INTO {load_table} VALUES("
        f"'{FID}',null,'{IMG_URL}',null,null,'{LICENSE}',null,null,"
        f"null,null,'{META_DATA_A}',null,null,'{PROVIDER}',null,null,null,null"
        f");"
    )

    load_data_query_b = (
        f"INSERT INTO {load_table} VALUES("
        f"'{FID}',null,'{IMG_URL}',null,null,'{LICENSE}',null,null,"
        f"null,null,'{META_DATA_B}',null,null,'{PROVIDER}',null,null,null,null"
        f");"
    )
    c = postgres_with_load_and_image_table.cursor
    conn = postgres_with_load_and_image_table.connection

    def log_col_values(table):
        c.execute(f"Select * FROM {table} LIMIT 0")
        colnames = [desc[0] for desc in c.description]
        c.execute(f"Select * FROM {table}")
        values = c.fetchall()[0]
        for i, cn in enumerate(colnames):
            print(f"{i} {cn} {values[i]}")

    c.execute(load_data_query_a)
    conn.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    conn.commit()
    c.execute(f"DELETE FROM {load_table};")
    conn.commit()
    c.execute(load_data_query_b)
    conn.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    conn.commit()

    logging.info("After UPSERT B")
    log_col_values(image_table)
    c.execute(f"SELECT * FROM {image_table};")
    actual_rows = c.fetchall()
    actual_row = actual_rows[0]
    assert len(actual_rows) == 1
    expected_meta_data = {
        "description": json.loads(META_DATA_B)["description"],
        "test": json.loads(META_DATA_A)["test"],
    }
    assert actual_row[16] == expected_meta_data


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

    load_data_query_a = (
        f"INSERT INTO {load_table} VALUES("
        f"'{FID}',null,'{IMG_URL}',null,null,'{LICENSE}',null,null,"
        f"null,null,null,'{TAGS_A}',null,'{PROVIDER}',null,null,null"
        f");"
    )

    load_data_query_b = (
        f"INSERT INTO {load_table} VALUES("
        f"'{FID}',null,'{IMG_URL}',null,null,'{LICENSE}',null,null,"
        f"null,null,null,'{TAGS_B}',null,'{PROVIDER}',null,null,null"
        f");"
    )
    c = postgres_with_load_and_image_table.cursor
    conn = postgres_with_load_and_image_table.connection
    c.execute(load_data_query_a)
    conn.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    conn.commit()
    c.execute(f"DELETE FROM {load_table};")
    conn.commit()
    c.execute(load_data_query_b)
    conn.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    conn.commit()
    c.execute(f"SELECT * FROM {image_table};")
    actual_rows = c.fetchall()
    actual_row = actual_rows[0]
    assert len(actual_rows) == 1
    expect_tags = [
        {"name": "tagone", "provider": "test"},
        {"name": "tagtwo", "provider": "test"},
        {"name": "tagthree", "provider": "test"},
    ]
    actual_tags = actual_row[17]
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

    load_data_query_a = (
        f"INSERT INTO {load_table} VALUES("
        f"'{FID}',null,'{IMG_URL}',null,null,'{LICENSE}',null,null,"
        f"null,null,null,'{json.dumps(TAGS)}',null,'{PROVIDER}',null,null,null"
        f");"
    )

    load_data_query_b = (
        f"INSERT INTO {load_table} VALUES("
        f"'{FID}',null,'{IMG_URL}',null,null,'{LICENSE}',null,null,"
        f"null,null,null,null,null,'{PROVIDER}',null,null,null"
        f");"
    )
    c = postgres_with_load_and_image_table.cursor
    conn = postgres_with_load_and_image_table.connection
    c.execute(load_data_query_a)
    conn.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    conn.commit()
    c.execute(f"DELETE FROM {load_table};")
    conn.commit()
    c.execute(load_data_query_b)
    conn.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    conn.commit()
    c.execute(f"SELECT * FROM {image_table};")
    actual_rows = c.fetchall()
    actual_row = actual_rows[0]
    assert len(actual_rows) == 1
    expect_tags = [
        {"name": "tagone", "provider": "test"},
        {"name": "tagtwo", "provider": "test"},
    ]
    actual_tags = actual_row[17]
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
    load_data_query_a = (
        f"INSERT INTO {load_table} VALUES("
        f"'{FID}',null,'{IMG_URL}',null,null,'{LICENSE}',null,null,"
        f"null,null,null,null,null,'{PROVIDER}',null,null,null"
        f");"
    )
    load_data_query_b = (
        f"INSERT INTO {load_table} VALUES("
        f"'{FID}',null,'{IMG_URL}',null,null,'{LICENSE}',null,null,"
        f"null,null,null,'{json.dumps(TAGS)}',null,'{PROVIDER}',null,null,null"
        f");"
    )
    c = postgres_with_load_and_image_table.cursor
    conn = postgres_with_load_and_image_table.connection
    c.execute(load_data_query_a)
    conn.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    conn.commit()
    c.execute(f"DELETE FROM {load_table};")
    conn.commit()
    c.execute(load_data_query_b)
    conn.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    conn.commit()
    c.execute(f"SELECT * FROM {image_table};")
    actual_rows = c.fetchall()
    actual_row = actual_rows[0]
    assert len(actual_rows) == 1
    expect_tags = [
        {"name": "tagone", "provider": "test"},
        {"name": "tagtwo", "provider": "test"},
    ]
    actual_tags = actual_row[17]
    assert len(actual_tags) == 2
    assert all([t in expect_tags for t in actual_tags])
    assert all([t in actual_tags for t in expect_tags])


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

    insert_data_query = (
        f"INSERT INTO {load_table} VALUES"
        f"('{FID_A}',null,'{IMG_URL_A}',null,null,'{LICENSE}',null,"
        f"null,'{CREATOR_URL_A}',null,null,'{json.dumps(TAGS)}',null,"
        f"'{PROVIDER}','{PROVIDER}',null,null),"
        f"('{FID_B}',null,'{IMG_URL_B}',null,null,'{LICENSE}',null,"
        f"null,'{CREATOR_URL_B}',null,null,'{json.dumps(TAGS)}',null,"
        f"'{PROVIDER}','{PROVIDER}',null,null);"
    )
    c = postgres_with_load_and_image_table.cursor
    conn = postgres_with_load_and_image_table.connection
    c.execute(insert_data_query)
    conn.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    conn.commit()
    c.execute(f"DELETE FROM {load_table};")
    conn.commit()

    sql.update_flickr_sub_providers(postgres_conn_id, image_table)
    conn.commit()
    c.execute(f"SELECT * FROM {image_table};")
    actual_rows = c.fetchall()
    assert len(actual_rows) == 2

    fid_index = 6
    source_index = 5
    for actual_row in actual_rows:
        if actual_row[fid_index] == "a":
            assert actual_row[source_index] == "nasa"
        else:
            assert actual_row[fid_index] == "b" and actual_row[source_index] == "flickr"


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

    insert_data_query = (
        f"INSERT INTO {load_table} VALUES"
        f"('{FID_A}',null,'{IMG_URL_A}',null,null,'{LICENSE}',null,"
        f"null,null,null,'{json.dumps(META_DATA_A)}',null,null,"
        f"'{PROVIDER}','{PROVIDER}',null,null),"
        f"('{FID_B}',null,'{IMG_URL_B}',null,null,'{LICENSE}',null,"
        f"null,null,null,'{json.dumps(META_DATA_B)}',null,null,"
        f"'{PROVIDER}','{PROVIDER}',null,null);"
    )

    c = postgres_with_load_and_image_table.cursor
    conn = postgres_with_load_and_image_table.connection
    c.execute(insert_data_query)
    conn.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    conn.commit()
    c.execute(f"DELETE FROM {load_table};")
    conn.commit()

    sql.update_europeana_sub_providers(postgres_conn_id, image_table)
    conn.commit()
    c.execute(f"SELECT * FROM {image_table};")
    actual_rows = c.fetchall()
    assert len(actual_rows) == 2

    fid_index = 6
    source_index = 5
    for actual_row in actual_rows:
        if actual_row[fid_index] == "a":
            assert actual_row[source_index] == "wellcome_collection"
        else:
            assert (
                actual_row[fid_index] == "b" and actual_row[source_index] == "europeana"
            )


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

    insert_data_query = (
        f"INSERT INTO {load_table} VALUES"
        f"('{FID_A}',null,'{IMG_URL_A}',null,null,'{LICENSE}',null,"
        f"null,null,null,'{json.dumps(META_DATA_A)}',null,null,"
        f"'{PROVIDER}','{PROVIDER}',null,null),"
        f"('{FID_B}',null,'{IMG_URL_B}',null,null,'{LICENSE}',null,"
        f"null,null,null,'{json.dumps(META_DATA_B)}',null,null,"
        f"'{PROVIDER}','{PROVIDER}',null,null);"
    )
    c = postgres_with_load_and_image_table.cursor
    conn = postgres_with_load_and_image_table.connection
    c.execute(insert_data_query)
    conn.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    conn.commit()
    c.execute(f"DELETE FROM {load_table};")
    conn.commit()

    sql.update_smithsonian_sub_providers(postgres_conn_id, image_table)
    conn.commit()
    c.execute(f"SELECT * FROM {image_table};")
    actual_rows = c.fetchall()
    assert len(actual_rows) == 2

    fid_index = 6
    source_index = 5
    for actual_row in actual_rows:
        if actual_row[fid_index] == "a":
            assert actual_row[source_index] == "smithsonian_institution_archives"
        else:
            assert (
                actual_row[fid_index] == "b"
                and actual_row[source_index]
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

    insert_data_query = (
        f"INSERT INTO {load_table} VALUES"
        f"('{FID_A}',null,'{IMG_URL_A}',null,null,'{LICENSE}',null,"
        f"null,null,null,null,null,null,"
        f"'{PROVIDER_A}','{PROVIDER_A}',null,null),"
        f"('{FID_B}',null,'{IMG_URL_B}',null,null,'{LICENSE}',null,"
        f"null,null,null,null,null,null,"
        f"'{PROVIDER_B}','{PROVIDER_B}',null,null);"
    )

    c = postgres_with_load_and_image_table.cursor
    conn = postgres_with_load_and_image_table.connection
    c.execute(insert_data_query)
    conn.commit()
    sql.upsert_records_to_db_table(postgres_conn_id, identifier, db_table=image_table)
    conn.commit()
    c.execute(f"DELETE FROM {load_table};")
    conn.commit()

    c.execute(
        f"UPDATE {image_table} SET updated_on = NOW() - INTERVAL '1 year' "
        f"WHERE provider = 'flickr';"
    )

    conn.commit()

    sql.expire_old_images(postgres_conn_id, PROVIDER_A, image_table=image_table)

    sql.expire_old_images(postgres_conn_id, PROVIDER_B, image_table=image_table)

    conn.commit()

    c.execute(f"SELECT * FROM {image_table};")
    actual_rows = c.fetchall()
    assert len(actual_rows) == 2

    fid_index = 6
    removed_index = 20
    logging.info(f"***Actual row 0 : {actual_rows[0]}")
    for actual_row in actual_rows:
        if actual_row[fid_index] == "a":
            assert not actual_row[removed_index]
        else:
            assert actual_row[fid_index] == "b" and actual_row[removed_index]

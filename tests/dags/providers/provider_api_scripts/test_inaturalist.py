from ast import literal_eval
from pathlib import Path

import pytest
from airflow.providers.postgres.hooks.postgres import PostgresHook
from common.constants import POSTGRES_CONN_ID
from providers.provider_api_scripts import inaturalist


# TO DO #898: Most of the transformations for inaturalist are in SQL, and testing them
# effectively could mean looking more closely at the production data itself.
# For now, sample data included in the tests below covers the following weird cases:
#  - 3 of the photo records link to unclassified observations, so they have no title or
#    tags and we don't load them.
#  - 10 of the remaining photo ids appear on multiple records, load one per photo_id.
#  - One of the photos has a taxon without any ancestors in the source table.
# To really get at data quality issues, it's worth loading bigger sample files to minio
# and running the dag in airflow locally.


INAT = inaturalist.INaturalistDataIngester()
PG = PostgresHook(POSTGRES_CONN_ID)
SQL_SCRIPT_DIR = (
    Path(__file__).parents[4]
    / "openverse_catalog/dags/providers/provider_csv_load_scripts/inaturalist"
)
RESOURCE_DIR = Path(__file__).parent / "resources/inaturalist"
# postgres always returns a list of tuples, which is not a valid json format
FULL_DB_RESPONSE = literal_eval((RESOURCE_DIR / "full_db_response.txt").read_text())
# in this case, it's a list containing a single tuple with a single value that is a
# valid json array of records for processing as if they came from a regular API.
JSON_RESPONSE = FULL_DB_RESPONSE[0][0]
RECORD0 = JSON_RESPONSE[0]


# Based on the small sample files in /tests/s3-data/inaturalist-open-data and on
# minio in the test environment
# Just checking raw record counts, assume that the contents load correctly as only
# taxa has any logic outside literally loading the table using postgres existing
# copy command.
@pytest.mark.parametrize(
    "file_name, expected",
    [
        ("create_schema.sql", [("inaturalist",)]),
        ("photos.sql", [(37,)]),
        ("observations.sql", [(32,)]),
        ("taxa.sql", [(183,)]),
        ("observers.sql", [(23,)]),
    ],
)
def test_load_data(file_name, expected):
    sql_string = (SQL_SCRIPT_DIR / file_name).read_text()
    actual = PG.get_records(sql_string)
    assert actual == expected


def test_get_next_query_params_no_prior():
    expected = {"offset_num": 0}
    actual = INAT.get_next_query_params()
    assert expected == actual


def test_get_next_query_params_prior_0():
    expected = {"offset_num": INAT.batch_limit}
    actual = INAT.get_next_query_params({"offset_num": 0})
    assert expected == actual


@pytest.mark.parametrize("value", [None, {}])
def test_get_batch_data_returns_none(value):
    with pytest.raises(
        NotImplementedError, match="TSV files from AWS S3 processed in postgres."
    ):
        INAT.get_batch_data(value)


def test_get_response_json():
    with pytest.raises(
        NotImplementedError, match="TSV files from AWS S3 processed in postgres."
    ):
        INAT.get_response_json({"offset_num": 0})


def test_get_batch_data_full_response():
    with pytest.raises(
        NotImplementedError, match="TSV files from AWS S3 processed in postgres."
    ):
        INAT.get_batch_data(JSON_RESPONSE)


@pytest.mark.parametrize("field", ["license_url", "foreign_identifier"])
def test_get_record_data_missing_necessarly_fields(field):
    with pytest.raises(
        NotImplementedError, match="TSV files from AWS S3 processed in postgres."
    ):
        INAT.get_record_data(RECORD0)


def test_get_record_data_full_response():
    with pytest.raises(
        NotImplementedError, match="TSV files from AWS S3 processed in postgres."
    ):
        INAT.get_record_data(RECORD0)


def test_get_media_type():
    expected = "image"
    actual = INAT.get_media_type({"some test": "data"})
    assert actual == expected

import os
from ast import literal_eval
from pathlib import Path

import psycopg2
import pytest
from common.licenses import get_license_info
from providers.provider_api_scripts import inaturalist


# Sample data included in the tests below covers the following weird cases:
#  - 3 of the photo records link to unclassified observations, so they have no title or
#    tags and we don't load them.
#  - 10 of the remaining photo ids appear on multiple records, load one per photo_id.
#  - One of the photos has a taxon without any ancestors in the source table.


INAT = inaturalist.INaturalistDataIngester()
CONNECTION_ID = os.getenv("AIRFLOW_CONN_POSTGRES_OPENLEDGER_TESTING")
RESOURCE_DIR = Path(__file__).parent / "resources/inaturalist"
# postgres always returns a list of tuples, which is not a valid json format
FULL_DB_RESPONSE = literal_eval((RESOURCE_DIR / "full_db_response.txt").read_text())
# in this case, it's a list containing a single tuple with a single value that is a
# valid json array of records for processing as if they came from a regular API.
JSON_RESPONSE = FULL_DB_RESPONSE[0][0]
RECORD0 = JSON_RESPONSE[0]


def test_load_data():
    # Based on the small sample files in /tests/s3-data/inaturalist-open-data and on
    # minio in the test environment
    # Just checking raw record counts, assume that the contents load correctly as only
    # taxa has any logic outside literally loading the table using postgres existing
    # copy command.
    SQL_SCRIPT_DIR = (
        Path(__file__).parents[4]
        / "openverse_catalog/dags/providers/provider_csv_load_scripts/inaturalist"
    )
    LOAD_STEPS = [
        ("00_create_schema.sql", [("inaturalist",)]),
        ("01_photos.sql", [(37,)]),
        ("02_observations.sql", [(32,)]),
        ("03_taxa.sql", [(183,)]),
        ("04_observers.sql", [(23,)]),
    ]
    db = psycopg2.connect(CONNECTION_ID)
    cursor = db.cursor()
    for (sql_script, expected) in LOAD_STEPS:
        sql_string = (SQL_SCRIPT_DIR / sql_script).read_text()
        cursor.execute(sql_string)
        actual = cursor.fetchall()
        assert actual == expected


def test_get_next_query_params_no_prior():
    expected = {"offset_num": 0}
    actual = INAT.get_next_query_params()
    assert expected == actual


def test_get_next_query_params_prior_0():
    expected = {"offset_num": 10_000}
    actual = INAT.get_next_query_params({"offset_num": 0})
    assert expected == actual


@pytest.mark.parametrize("value", [None, {}])
def test_get_batch_data_returns_none(value):
    actual = INAT.get_batch_data(value)
    assert actual is None


def test_get_response_json():
    expected = JSON_RESPONSE
    actual = INAT.get_response_json({"offset_num": 0})
    assert actual == expected


def test_get_batch_data_full_response():
    actual = INAT.get_batch_data(JSON_RESPONSE)
    assert isinstance(actual, list)
    assert len(actual) == 34
    assert isinstance(actual[0], dict)
    assert actual[0] == RECORD0


@pytest.mark.parametrize("field", ["license_url", "foreign_identifier"])
def test_get_record_data_missing_necessarly_fields(field):
    expected = None
    record = RECORD0.copy()
    record.pop(field)
    actual = INAT.get_record_data(record)
    assert actual == expected


def test_get_record_data_full_response():
    expected = {
        "foreign_identifier": 10314159,
        "filetype": "jpg",
        "license_info": get_license_info(
            license_url="http://creativecommons.org/licenses/by-nc/4.0/"
        ),
        "width": 1530,
        "height": 2048,
        "foreign_landing_url": "https://www.inaturalist.org/photos/10314159",
        "image_url": "https://inaturalist-open-data.s3.amazonaws.com/photos/10314159/medium.jpg",
        "creator": "akjenny",
        "creator_url": "https://www.inaturalist.org/users/615549",
        "title": "Trifolium hybridum",
        "raw_tags": [
            "Fabaceae",
            "Fabales",
            "Magnoliopsida",
            "Angiospermae",
            "Plantae",
            "Trifolium",
            "Tracheophyta",
            "Faboideae",
            "Trifolieae",
        ],
    }
    actual = INAT.get_record_data(RECORD0)
    assert actual == expected


def test_get_media_type():
    expected = "image"
    actual = INAT.get_media_type(INAT.get_record_data(RECORD0))
    assert actual == expected

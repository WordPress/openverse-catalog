import logging
from ast import literal_eval
from pathlib import Path

import pytest
from common.licenses import get_license_info
from providers.provider_api_scripts import inaturalist


# This file supports testing of the customized functions within the provider ingestion
# class. It does not cover the pre-ingestion steps or sqls. To test those, instantiate
# the local dev environment, with the inaturalist-open-data bucket on the minio docker.
# (`just recreate` should do the trick.) Then manually kick off the Airflow DAG, and
# inspect the logs and the data saved to openverse s3 on minio.

# Based on the small sample files in /tests/s3-data/inaturalist-open-data, expect:
# taxa.csv.gz --> 183 records
# observations.csv.gz ---> 31 records
# observers.csv.gz ---> 22 records
# photos.csv.gz ---> 36 records
# 3 of the photo records link to unclassified observations, so they have no title or
# tags and we don't load them.
# 10 of the remaining photo ids appear on multiple records, load one per photo_id.
# The expected response to the database query for transformed data is a list of tuples
# (not technically json, though the first value in each tuple is a dict) in
# ./resources/inaturalist/full_db_response.txt

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


INAT = inaturalist.inaturalistDataIngester()
RESOURCE_DIR = Path(__file__).parent / "resources/inaturalist"
FULL_DB_RESPONSE = literal_eval((RESOURCE_DIR / "full_db_response.txt").read_text())
RECORD0 = FULL_DB_RESPONSE[0][0]


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
    actual = INAT.get_batch_data(value)
    assert actual is None


def test_get_batch_data_full_response():
    actual = INAT.get_batch_data(FULL_DB_RESPONSE)
    assert isinstance(actual, list)
    assert len(actual) == 33
    assert isinstance(actual[0], dict)
    assert actual[0] == RECORD0


def test_get_record_data_no_license():
    expected = None
    record = RECORD0.copy()
    record.pop("license_url")
    actual = INAT.get_record_data(record)
    assert actual == expected


def test_get_record_data_no_foreign_id():
    expected = None
    record = RECORD0.copy()
    record.pop("foreign_id")
    actual = INAT.get_record_data(record)
    assert actual == expected


def test_get_record_data_full_response():
    expected = {
        "foreign_identifier": "10314159",
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
        "raw_tags": "Tracheophyta; Angiospermae; Magnoliopsida; Fabales; Fabaceae; Faboideae; Trifolieae; Trifolium",
    }
    actual = INAT.get_record_data(RECORD0)
    assert actual == expected


def test_get_media_type():
    expected = "image"
    actual = INAT.get_media_type(INAT.get_record_data(RECORD0))
    assert actual == expected

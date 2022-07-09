import logging

import pytest
from common.licenses import get_license_info
from providers.provider_api_scripts import inaturalist


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


# Below are record counts from the sample files in tests/s3-data/inaturalist-open-data
# 33 of the photo records link to unclassified observations, so they have no title or
# tags, so we don't load them.
# ---> Reading file taxa.csv.gz
# 183 records
# ---> Reading file observations.csv.gz
# 31 records
# ---> Reading file observers.csv.gz
# 22 records
# ---> Reading file photos.csv.gz
# 36 records

INAT = inaturalist.inaturalistDataIngester()
RECORD0 = {
    "foreign_id": 191018903,
    "filetype": "jpg",
    "license_url": "http://creativecommons.org/licenses/by-nc/4.0/",
    "width": 818,
    "height": 741,
    "foreign_landing_url": "https://www.inaturalist.org/photos/191018903",
    "image_url": "https://inaturalist-open-data.s3.amazonaws.com/photos/191018903/medium.jpg",
    "creator": "tiwane",
    "creator_url": "https://www.inaturalist.org/users/28",
    "title": "Ranunculus occidentalis",
    "tags": "Tracheophyta; Angiospermae; Magnoliopsida; Ranunculales; Ranunculaceae; Ranunculoideae; Ranunculeae; Ranunculus",
}


def test_get_next_query_params_no_prior():
    expect = {"page_number": 0}
    actual = INAT.get_next_query_params()
    assert expect == actual


def test_get_next_query_params_prior_0():
    expect = {"page_number": 1}
    actual = INAT.get_next_query_params({"page_number": 0})
    assert expect == actual


@pytest.mark.parametrize(
    "file_name, expect",
    [
        ("00_create_schema.sql", [("inaturalist",)]),
        ("03_taxa.sql", [(183,)]),
        ("04_observers.sql", [(22,)]),
    ],
)
def test_sql_runner(file_name, expect):
    actual = inaturalist.sql_runner(file_name)
    assert actual == expect


def test_load_photos():
    expect = [(36,)]
    actual = inaturalist.load_photos()
    assert actual == expect


def test_load_observations():
    expect = [(31,)]
    actual = inaturalist.load_observations()
    assert actual == expect


def test_get_response_json():
    expect0 = RECORD0
    actual = INAT.get_response_json({"page_number": 0})
    assert isinstance(actual, list)
    assert len(actual) == 33
    assert actual[0][0] == expect0


def test_get_batch_data_none_response():
    expect = None
    actual = INAT.get_batch_data(None)
    assert actual == expect


def test_get_batch_data_empty_response():
    expect = None
    actual = INAT.get_batch_data({})
    assert actual == expect


def test_get_batch_data_full_response():
    expect0 = RECORD0
    actual = INAT.get_batch_data(INAT.get_response_json({"page_number": 0}))
    assert isinstance(actual, list)
    assert len(actual) == 33
    assert isinstance(actual[0], dict)
    assert actual[0] == expect0


def test_get_record_data_no_license():
    expect = None
    record = RECORD0.copy()
    record.pop("license_url")
    actual = INAT.get_record_data(record)
    assert actual == expect


def test_get_record_data_no_foreign_id():
    expect = None
    record = RECORD0.copy()
    record.pop("foreign_id")
    actual = INAT.get_record_data(record)
    assert actual == expect


def test_get_record_data_full_response():
    expect = {
        "foreign_identifier": "191018903",
        "filetype": "jpg",
        "license_info": get_license_info(
            license_url="http://creativecommons.org/licenses/by-nc/4.0/"
        ),
        "width": 818,
        "height": 741,
        "foreign_landing_url": "https://www.inaturalist.org/photos/191018903",
        "image_url": "https://inaturalist-open-data.s3.amazonaws.com/photos/191018903/medium.jpg",
        "creator": "tiwane",
        "creator_url": "https://www.inaturalist.org/users/28",
        "title": "Ranunculus occidentalis",
        "raw_tags": "Tracheophyta; Angiospermae; Magnoliopsida; Ranunculales; Ranunculaceae; Ranunculoideae; Ranunculeae; Ranunculus",
    }
    actual = INAT.get_record_data(RECORD0)
    assert actual == expect


def test_get_media_type():
    expect = "image"
    actual = INAT.get_media_type(INAT.get_record_data(RECORD0))
    assert actual == expect

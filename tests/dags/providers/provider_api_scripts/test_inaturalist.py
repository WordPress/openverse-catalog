import logging

import pytest
from common.licenses import get_license_info
from providers.provider_api_scripts.inaturalist import inaturalistDataIngester


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


# Below are record counts from the sample files in tests/s3-data/inaturalist-open-data
# 3 of the photo records link to unclassified observations, so they have no title or
# tags, so we don't load them.
# ---> Reading file taxa.csv.gz
# 183 records
# ---> Reading file observations.csv.gz
# 31 records
# ---> Reading file observers.csv.gz
# 22 records
# ---> Reading file photos.csv.gz
# 36 records

INAT = inaturalistDataIngester()
RECORD0 = {
    "foreign_id": 10314159,
    "filetype": "jpg",
    "license_url": "http://creativecommons.org/licenses/by-nc/4.0/",
    "width" : 1530,
    "height" : 2048,
    "foreign_landing_url": "https://www.inaturalist.org/photos/10314159",
    "image_url": "https://inaturalist-open-data.s3.amazonaws.com/photos/10314159/medium.jpg",
    "creator": "akjenny",
    "creator_url": "https://www.inaturalist.org/users/615549",
    "title" : "Trifolium hybridum",
    "tags" : "Tracheophyta; Angiospermae; Magnoliopsida; Fabales; Fabaceae; Faboideae; Trifolieae; Trifolium",
}


def test_get_next_query_params_no_prior():
    expected = {"offset_num": 0}
    actual = INAT.get_next_query_params()
    assert expected == actual


def test_get_next_query_params_prior_0():
    expected = {"offset_num": 100}
    actual = INAT.get_next_query_params({"offset_num": 0})
    assert expected == actual


@pytest.mark.parametrize(
    "file_name, expected",
    [
        ("00_create_schema.sql", [("inaturalist",)]),
        ("01_photos.sql", [(36,)]),
        ("02_observations.sql", [(31,)]),
        ("03_taxa.sql", [(183,)]),
        ("04_observers.sql", [(22,)]),
    ],
)
def test_sql_loader(file_name, expected):
    actual = INAT.sql_loader(file_name)
    assert actual == expected


def test_get_response_json():
    actual = INAT.get_response_json({"offset_num": 0})
    assert isinstance(actual, list)
    assert len(actual) == 33
    assert actual[0][0] == RECORD0


def test_get_batch_data_none_response():
    expected = None
    actual = INAT.get_batch_data(None)
    assert actual == expected


def test_get_batch_data_empty_response():
    expected = None
    actual = INAT.get_batch_data({})
    assert actual == expected


def test_get_batch_data_full_response():
    actual = INAT.get_batch_data(INAT.get_response_json({"offset_num": 0}))
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
        "width" : 1530,
        "height" : 2048,
        "foreign_landing_url": "https://www.inaturalist.org/photos/10314159",
        "image_url": "https://inaturalist-open-data.s3.amazonaws.com/photos/10314159/medium.jpg",
        "creator": "akjenny",
        "creator_url": "https://www.inaturalist.org/users/615549",
        "title" : "Trifolium hybridum",
        "raw_tags" : "Tracheophyta; Angiospermae; Magnoliopsida; Fabales; Fabaceae; Faboideae; Trifolieae; Trifolium",
    }
    actual = INAT.get_record_data(RECORD0)
    assert actual == expected


def test_get_media_type():
    expected = "image"
    actual = INAT.get_media_type(INAT.get_record_data(RECORD0))
    assert actual == expected

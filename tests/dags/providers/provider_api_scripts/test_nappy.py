import json
from ast import literal_eval
from pathlib import Path

import pytest
from common.constants import IMAGE
from common.licenses import get_license_info
from providers.provider_api_scripts.nappy import NappyDataIngester


# resource files
RESOURCES = Path(__file__).parent / "resources/nappy"
FULL_BATCH_RESPONSE = json.loads((RESOURCES / "images.json").read_text())
SINGLE_ITEM = literal_eval((RESOURCES / "single_item.json").read_text())

# Set up test class
ingester = NappyDataIngester()


def test_get_next_query_params_default_response():
    actual_result = ingester.get_next_query_params(None)
    expected_result = {"page": 1}
    assert actual_result == expected_result


def test_get_next_query_params_updates_parameters():
    previous_query_params = {"page": 42}
    actual_result = ingester.get_next_query_params(previous_query_params)
    expected_result = {"page": 43}
    assert actual_result == expected_result


# this is based on the assumption that Nappy will only ever send us image data
@pytest.mark.parametrize(
    "record",
    [None, {}, {"here is": "some data"}],
)
def test_get_media_type(record):
    expected_result = IMAGE
    actual_result = ingester.get_media_type(record)
    assert actual_result == expected_result


@pytest.mark.parametrize(
    "response_json, expected",
    [
        pytest.param(
            FULL_BATCH_RESPONSE,
            FULL_BATCH_RESPONSE["images"],
            id="happy_path",
        ),
        pytest.param({}, None, id="empty_dict"),
        pytest.param(None, None, id="None"),
    ],
)
def test_get_batch_data(response_json, expected):
    actual = ingester.get_batch_data(response_json)
    assert actual == expected


@pytest.mark.parametrize(
    "response_json, expected_result",
    [
        ({}, False),
        (FULL_BATCH_RESPONSE, True),
        (SINGLE_ITEM, False),
    ],
)
def test_get_should_continue(response_json, expected_result):
    actual_result = ingester.get_should_continue(response_json)
    assert actual_result == expected_result


# def get_record_data(self, data: dict) -> dict | list[dict] | None:
@pytest.mark.parametrize(
    "response_json, expected_data",
    [
        pytest.param({}, None, id="empty_dict"),
        pytest.param(FULL_BATCH_RESPONSE, None, id="no_urls"),
        pytest.param(
            SINGLE_ITEM,
            {
                "foreign_landing_url": "https://nappy.co/photo/9/woman-with-tattoos",
                "image_url": "https://images.nappy.co/uploads/large/101591721349meykm7s6hvaswwvslpjrwibeyzru1fcxtxh0hf09cs7kdhmtptef4y3k4ua5z1bkyrbxov8tmagnafm8upwa3hxaxururtx7azaf.jpg",
                "license_info": get_license_info(
                    "https://creativecommons.org/publicdomain/zero/1.0/"
                ),
                "foreign_identifier": 9,
                "filesize": 233500,
                "filetype": "jpg",
                "creator": "iamconnorrm",
                "creator_url": "https://nappy.co/iamconnorrm",
                "title": "woman with tattoos",
                "thumbnail_url": "https://images.nappy.co/uploads/large/101591721349meykm7s6hvaswwvslpjrwibeyzru1fcxtxh0hf09cs7kdhmtptef4y3k4ua5z1bkyrbxov8tmagnafm8upwa3hxaxururtx7azaf.jpg?auto=format&w=600&q=75",
                "meta_data": {
                    "views": 82692,
                    "saves": 18,
                    "downloads": 1329,
                },
                "raw_tags": [
                    "indoor",
                    "bed",
                    "arthropod",
                    "dark",
                    "lobster",
                    "braids",
                    "female",
                    "red",
                    "blue",
                    "tattoo",
                    "earring",
                    "phone",
                    "laying",
                    "room",
                ],
                "width": 2048,
                "height": 1361,
            },
            id="happy_path",
        ),
    ],
)
def test_get_record_data(response_json, expected_data):
    actual_data = ingester.get_record_data(response_json)
    assert actual_data == expected_data
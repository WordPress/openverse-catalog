import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests
from common.licenses import LicenseInfo
from common.storage.image import MockImageStore
from providers.provider_api_scripts import cleveland_museum_of_art as clm


@pytest.fixture(autouse=True)
def validate_url_string():
    with patch("common.urls.validate_url_string") as mock_validate_url_string:
        mock_validate_url_string.side_effect = lambda x: x
        yield


_license_info = (
    "cc0",
    "1.0",
    "https://creativecommons.org/publicdomain/zero/1.0/",
    None,
)
CC0_LICENSE = LicenseInfo(*_license_info)
clm.image_store = MockImageStore(
    provider=clm.PROVIDER,
    license_info=CC0_LICENSE,
)

RESOURCES = Path(__file__).parent / "resources/clevelandmuseum"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.DEBUG
)


def _get_resource_json(json_name):
    with open(RESOURCES / json_name) as f:
        resource_json = json.load(f)
        return resource_json


def test_build_query_param_default():
    actual_param = clm._build_query_param()
    expected_param = {"cc": "1", "has_image": "1", "limit": 1000, "skip": 0}
    assert actual_param == expected_param


def test_build_query_param_with_givens():
    actual_param = clm._build_query_param(offset=1000)
    expected_param = {"cc": "1", "has_image": "1", "limit": 1000, "skip": 1000}
    assert actual_param == expected_param


def test_get_image_type_web():
    response_json = _get_resource_json("response_success.json")
    image_data = _get_resource_json("image_type_web.json")
    data = response_json["data"][0]
    data["images"] = image_data
    actual_image = clm._handle_batch_item(data)

    expected_url = "https://openaccess-cdn.clevelandart.org/1335.1917/1335.1917_web.jpg"
    expected_width = 1263
    expected_height = 775
    expected_filetype = "jpg"
    expected_filesize = 716717

    assert actual_image["image_url"] == expected_url
    assert actual_image["width"] == expected_width
    assert actual_image["height"] == expected_height
    assert actual_image["filetype"] == expected_filetype
    assert actual_image["filesize"] == expected_filesize


def test_get_image_type_print():
    response_json = _get_resource_json("response_success.json")
    image_data = _get_resource_json("image_type_print.json")
    data = response_json["data"][0]
    data["images"] = image_data
    actual_image = clm._handle_batch_item(data)

    expected_url = (
        "https://openaccess-cdn.clevelandart.org/1335.1917/1335.1917_print.jpg"
    )
    expected_width = 3400
    expected_height = 2086
    expected_filetype = "jpg"
    expected_filesize = 5582485

    assert actual_image["image_url"] == expected_url
    assert actual_image["width"] == expected_width
    assert actual_image["height"] == expected_height
    assert actual_image["filetype"] == expected_filetype
    assert actual_image["filesize"] == expected_filesize


def test_get_image_type_full():
    response_json = _get_resource_json("response_success.json")
    image_data = _get_resource_json("image_type_full.json")
    data = response_json["data"][0]
    data["images"] = image_data
    actual_image = clm._handle_batch_item(data)

    expected_url = (
        "https://openaccess-cdn.clevelandart.org/1335.1917/1335.1917_full.tif"
    )
    expected_width = 6280
    expected_height = 3853
    expected_filetype = "tif"
    expected_filesize = 72628688

    assert actual_image["image_url"] == expected_url
    assert actual_image["width"] == expected_width
    assert actual_image["height"] == expected_height
    assert actual_image["filetype"] == expected_filetype
    assert actual_image["filesize"] == expected_filesize


def test_get_image_type_none():
    response_json = _get_resource_json("response_success.json")
    image_data = _get_resource_json("image_type_none.json")
    data = response_json["data"][0]
    data["images"] = image_data
    actual_image = clm._handle_batch_item(data)

    assert actual_image is None


def test_get_metadata():
    data = _get_resource_json("complete_data.json")
    actual_metadata = clm._get_metadata(data)
    expected_metadata = _get_resource_json("expect_metadata.json")
    assert actual_metadata == expected_metadata


def test_get_response_success():
    query_param = {"cc": 1, "has_image": 1, "limit": 1, "skip": 30000}
    response_json = _get_resource_json("response_success.json")
    r = requests.Response()
    r.status_code = 200
    r.json = MagicMock(return_value=response_json)
    with patch.object(clm.delay_request, "get", return_value=r) as mock_get:
        response_json, total_images = clm._get_response(query_param)
    expected_response = _get_resource_json("response_success.json")

    assert mock_get.call_count == 1
    assert response_json == expected_response
    assert total_images == 1


def test_get_response_no_data():
    query_param = {"cc": 1, "has_image": 1, "limit": 1, "skip": 33000}
    response_json = _get_resource_json("response_no_data.json")
    r = requests.Response()
    r.status_code = 200
    r.json = MagicMock(return_value=response_json)
    with patch.object(clm.delay_request, "get", return_value=r) as mock_get:
        response_json, total_images = clm._get_response(query_param)
    expected_response = _get_resource_json("response_no_data.json")

    assert mock_get.call_count == 1
    assert response_json == expected_response
    assert total_images == 0


def test_get_response_failure():
    query_param = {"cc": 1, "has_image": 1, "limit": 1, "skip": -1}
    r = requests.Response()
    r.status_code = 500
    r.json = None
    with patch.object(clm.delay_request, "get", return_value=r) as mock_get:
        clm._get_response(query_param)

    assert mock_get.call_count == 3


def test_handle_single_response():
    response_json = _get_resource_json("response_success.json")
    data = response_json["data"]
    clm._handle_response(data)
    actual_image = clm.image_store.media_buffer[0]
    expected_image = {
        "creator": "",
        "foreign_identifier": "96887",
        "foreign_landing_url": "https://clevelandart.org/art/1916.586.a",
        "width": 641,
        "height": 900,
        "filesize": 222248,
        "filetype": "jpg",
        "url": "https://openaccess-cdn.clevelandart.org/1916.586.a/1916.586.a_web.jpg",
        "license": CC0_LICENSE.license,
        "license_version": CC0_LICENSE.version,
        "meta_data": {
            "accession_number": "1916.586.a",
            "classification": "Miscellaneous",
            "credit_line": "Gift of Mr. and Mrs. J. H. Wade",
            "culture": "Germany, 18th century",
            "date": "1700s",
            "tombstone": "Scent Bottle, 1700s. Germany, 18th century. Glass with "
            "enamel decoration; overall: 10.2 cm (4 in.). The Cleveland "
            "Museum of Art, Gift of Mr. and Mrs. J. H. Wade 1916.586.a",
            "technique": "glass with enamel decoration",
        },
        "title": "Scent Bottle",
    }
    for key, value in expected_image.items():
        assert getattr(actual_image, key) == value


def test_get_response_None():
    query_param = {"cc": 1, "has_image": 1, "limit": 1, "skip": -1}
    with patch.object(clm.delay_request, "get", return_value=None) as mock_get:
        response_json, total_images = clm._get_response(query_param)

    assert response_json is None
    assert total_images == 0
    # Retries
    assert mock_get.call_count == 3


def test_handle_response():
    response_json = _get_resource_json("handle_response_data.json")
    data = response_json["data"]
    actual_total_images = clm._handle_response(data)
    expected_total_images = 100

    assert actual_total_images == expected_total_images

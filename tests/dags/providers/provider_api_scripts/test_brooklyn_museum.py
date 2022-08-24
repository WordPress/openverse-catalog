import json
from pathlib import Path
from unittest.mock import patch

import pytest
from common.licenses import LicenseInfo
from common.loader import provider_details as prov
from common.storage.image import ImageStore
from providers.provider_api_scripts.brooklyn_museum import BrooklynMuseumDataIngester


RESOURCES = Path(__file__).parent / "resources/brooklynmuseum"
bkm = BrooklynMuseumDataIngester()
image_store = ImageStore(provider=prov.BROOKLYN_DEFAULT_PROVIDER)
bkm.media_stores = {"image": image_store}


def _get_resource_json(json_name):
    return json.loads((RESOURCES / json_name).read_text())


def test_build_query_param_default():
    actual_param = bkm.get_next_query_params(None)
    expected_param = {
        "has_images": 1,
        "rights_type_permissive": 1,
        "limit": bkm.batch_limit,
        "offset": 0,
    }
    assert actual_param == expected_param


def test_build_query_param_given():
    offset = 70
    actual_param = bkm.get_next_query_params({"foo": "bar"}, offset=offset)
    expected_param = {
        "has_images": 1,
        "rights_type_permissive": 1,
        "limit": bkm.batch_limit,
        "offset": offset + bkm.batch_limit,
    }
    assert actual_param == expected_param


@pytest.mark.parametrize(
    "resource_name, expected",
    [
        ("response_success.json", _get_resource_json("response_success.json")["data"]),
        ("response_error.json", None),
        ("response_nodata.json", []),
    ],
)
def test_get_data_from_response(resource_name, expected):
    response_json = _get_resource_json(resource_name)
    actual = bkm._get_data_from_response(response_json)
    assert actual == expected


@pytest.mark.parametrize(
    "resource_name, expected",
    [
        ("response_success.json", True),
        ("response_error.json", False),
        ("response_nodata.json", False),
    ],
)
def test_should_continue(resource_name, expected):
    response_json = _get_resource_json(resource_name)
    actual = bkm.get_should_continue(response_json)
    assert actual == expected


@pytest.mark.parametrize(
    "batch_objects_name, object_data_name, expected_count",
    [
        ("batch_objects.json", "object_data.json", 1),
        ("no_batch_objects.json", "non_cc_object_data.json", 0),
    ],
)
def test_process_batch(batch_objects_name, object_data_name, expected_count):
    batch_objects = _get_resource_json(batch_objects_name)
    response_json = _get_resource_json(object_data_name)

    with (
        patch.object(bkm, "get_response_json"),
        patch.object(bkm, "_get_data_from_response", return_value=response_json),
        patch.object(image_store, "add_item"),
    ):
        actual_count = bkm.process_batch(batch_objects)

    assert actual_count == expected_count


@pytest.mark.parametrize(
    "resource_name, expected",
    [
        (
            "object_data.json",
            [
                {
                    "creator": None,
                    "foreign_identifier": 170425,
                    "foreign_landing_url": "https://www.brooklynmuseum.org/opencollection/objects/90636",
                    "height": 1152,
                    "image_url": "https://d1lfxha3ugu3d4.cloudfront.net/images/opencollection/objects/size4/CUR.66.242.29.jpg",
                    "license_info": LicenseInfo(
                        license="by",
                        version="3.0",
                        url="https://creativecommons.org/licenses/by/3.0/",
                        raw_url="https://creativecommons.org/licenses/by/3.0/",
                    ),
                    "meta_data": {
                        "accession_number": "66.242.29",
                        "classification": "Clothing",
                        "credit_line": "Gift of John C. Monks",
                        "date": None,
                        "description": None,
                        "medium": "Silk",
                    },
                    "title": "Caftan",
                    "width": 1536,
                },
            ],
        ),
        ("object_data_noimage.json", []),
    ],
)
def test_handle_object_data(resource_name, expected):
    response_json = _get_resource_json(resource_name)
    license_url = "https://creativecommons.org/licenses/by/3.0/"

    actual = bkm._handle_object_data(response_json, license_url)
    assert actual == expected


@pytest.mark.parametrize(
    "resource_name, expected",
    [
        ("image_details.json", (1152, 1536)),
        ("image_nosize.json", (None, None)),
    ],
)
def test_get_image_size(resource_name, expected):
    response_json = _get_resource_json(resource_name)
    actual = bkm._get_image_sizes(response_json)

    assert actual == expected


@pytest.mark.parametrize(
    "resource_name, expected",
    [
        ("cc_license_info.json", "https://creativecommons.org/licenses/by/3.0/"),
        (
            "public_license_info.json",
            "https://creativecommons.org/publicdomain/zero/1.0/",
        ),
        ("no_license_info.json", None),
    ],
)
def test_get_license_url(resource_name, expected):
    response_json = _get_resource_json(resource_name)
    actual = bkm._get_license_url(response_json)

    assert actual == expected


def test_get_metadata():
    response_json = _get_resource_json("object_data.json")
    actual_metadata = bkm._get_metadata(response_json)
    expected_metadata = _get_resource_json("metadata.json")

    assert actual_metadata == expected_metadata


@pytest.mark.parametrize(
    "data, expected",
    [
        (_get_resource_json("artists_details.json"), "John La Farge"),
        ({}, None),
    ],
)
def test_get_creators(data, expected):
    actual = bkm._get_creators(data)
    assert actual == expected


@pytest.mark.parametrize(
    "data, expected",
    [
        (
            _get_resource_json("image_details.json"),
            "https://d1lfxha3ugu3d4.cloudfront.net/images/opencollection/objects/"
            "size4/CUR.66.242.29.jpg",
        ),
        ({}, None),
    ],
)
def test_get_images(data, expected):
    actual = bkm._get_image_url(data)
    assert actual == expected

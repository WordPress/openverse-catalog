import json
from pathlib import Path
from unittest.mock import patch

import pytest
from common.licenses import get_license_info
from providers.provider_api_scripts.phylopic import PhylopicDataIngester


RESOURCES = Path(__file__).parent / "resources/phylopic"
pp = PhylopicDataIngester()


def get_json(filename):
    with open(RESOURCES / filename) as f:
        return json.load(f)


def test__get_initial_query_params():
    with patch.object(pp, "get_response_json", return_value={}), pytest.raises(
        Exception
    ):
        pp._get_initial_query_params()

    data = get_json("initial_request.json")
    with patch.object(pp, "get_response_json", return_value=data):
        pp._get_initial_query_params()

    assert pp.build_param == 194
    assert pp.total_pages == 145


@pytest.mark.parametrize(
    "build_param, current_page, expected_query_params",
    [
        (111, 0, {"build": 111, "page": 0}),
        (222, 2, {"build": 222, "page": 3}),
    ],
)
def test_get_next_query_params(build_param, current_page, expected_query_params):
    pp.build_param = build_param
    pp.current_page = current_page

    prev_query_params = None
    if current_page != 0:
        prev_query_params = {
            "build": pp.build_param,
            "page": pp.current_page,
            "embed_items": "true",
        }
    actual_query_params = pp.get_next_query_params(prev_query_params)

    assert actual_query_params == expected_query_params | {"embed_items": "true"}


def test_get_record_data():
    data = get_json("sample_record.json")
    image = pp.get_record_data(data)
    license_info = get_license_info(
        license_url="https://creativecommons.org/publicdomain/zero/1.0/"
    )

    assert image == {
        "license_info": license_info,
        "foreign_identifier": "5b1e88b5-159d-495d-b8cb-04f9e28d2f02",
        "foreign_landing_url": "https://www.phylopic.org/images/5b1e88b5-159d-495d-b8cb-04f9e28d2f02?build=194",
        "image_url": "https://images.phylopic.org/images/5b1e88b5-159d-495d-b8cb-04f9e28d2f02/source.svg",
        "title": "Hemaris tityus",
        "creator": "Andy Wilson",
        "creator_url": "https://www.phylopic.org/contributors/c3ac6939-e85a-4a10-99d1-4079537f34de?build=194",
        "width": 2048,
        "height": 2048,
    }

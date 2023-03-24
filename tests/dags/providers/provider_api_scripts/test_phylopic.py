import json
from pathlib import Path
from unittest.mock import patch

import pytest

# from common.licenses import LicenseInfo
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

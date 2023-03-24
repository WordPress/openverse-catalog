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

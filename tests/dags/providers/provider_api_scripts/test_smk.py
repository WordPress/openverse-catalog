import json
import logging
from pathlib import Path

# from unittest.mock import MagicMock, patch
# import requests
from common.licenses import LicenseInfo
from providers.provider_api_scripts.smk import SmkDataIngester


RESOURCES = Path(__file__).parent.resolve() / "resources/smk"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.DEBUG
)

smk = SmkDataIngester()

CC0 = LicenseInfo(
    "cc0",
    "1.0",
    "https://creativecommons.org/publicdomain/zero/1.0/",
    None,
)


def _get_resource_json(json_name):
    with open(RESOURCES / json_name) as f:
        resource_json = json.load(f)
    return resource_json


def test_get_next_query_params_first_call():
    actual_param = smk.get_next_query_params(prev_query_params=None)
    expected_param = {
        "keys": "*",
        "filters": "[has_image:true],[public_domain:true]",
        "offset": 0,
        "rows": 2000,
    }

    assert actual_param == expected_param


def test_get_next_query_params_increments_offset():
    actual_param = smk.get_next_query_params(
        {
            "keys": "*",
            "filters": "[has_image:true],[public_domain:true]",
            "offset": 0,
            "rows": 2000,
        }
    )
    expected_param = {
        "keys": "*",
        "filters": "[has_image:true],[public_domain:true]",
        "offset": 2000,
        "rows": 2000,
    }

    assert actual_param == expected_param

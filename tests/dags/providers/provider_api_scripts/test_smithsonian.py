# import json
from pathlib import Path
from unittest.mock import patch

# import pytest
from providers.provider_api_scripts.smithsonian import SmithsonianDataIngester


RESOURCES = Path(__file__).parent / "tests/resources/smithsonian"

# Set up test class
ingester = SmithsonianDataIngester()


def test_get_hash_prefixes_with_len_one():
    ingester.hash_prefix_length = 1
    expect_prefix_list = [
        "0",
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
        "a",
        "b",
        "c",
        "d",
        "e",
        "f",
    ]
    actual_prefix_list = list(ingester._get_hash_prefixes())
    assert actual_prefix_list == expect_prefix_list
    ingester.hash_prefix_length = 2  # Undo the change


def test_alert_new_unit_codes():
    sub_prov_dict = {"sub_prov1": {"a", "c"}, "sub_prov2": {"b"}, "sub_prov3": {"e"}}
    unit_code_set = {"a", "b", "c", "d"}
    with patch.dict(ingester.sub_providers, sub_prov_dict, clear=True):
        actual_codes = ingester._get_new_and_outdated_unit_codes(unit_code_set)
    expected_codes = ({"d"}, {"e"})
    assert actual_codes == expected_codes


def test_get_next_query_params_first_call():
    hash_prefix = "ff"
    actual_params = ingester.get_next_query_params(
        prev_query_params=None, hash_prefix=hash_prefix
    )
    actual_params.pop("api_key")  # Omitting the API key
    expected_params = {
        "q": f"online_media_type:Images AND media_usage:CC0 AND hash:{hash_prefix}*",
        "start": 0,
        "rows": 1000,
    }
    assert actual_params == expected_params


def test_get_next_query_params_updates_parameters():
    previous_query_params = {
        "api_key": "pass123",
        "q": "online_media_type:Images AND media_usage:CC0 AND hash:ff*",
        "start": 0,
        "rows": 1000,
    }
    new_hash_prefix = "01"
    actual_params = ingester.get_next_query_params(
        previous_query_params, hash_prefix=new_hash_prefix
    )
    expected_params = {
        "api_key": "pass123",
        "q": f"online_media_type:Images AND media_usage:CC0 AND hash:{new_hash_prefix}*",
        "start": 1000,
        "rows": 1000,
    }
    assert actual_params == expected_params


def test_get_media_type():
    actual_type = ingester.get_media_type(record={})
    assert actual_type == "image"


# def test_get_record_data():
#     # High level test for `get_record_data`. One way to test this is to create a
#     # `tests/resources/Smithsonian/single_item.json` file containing a sample json
#     # representation of a record from the API under test, call `get_record_data` with
#     # the json, and directly compare to expected output.
#     #
#     # Make sure to add additional tests for records of each media type supported by
#     # your provider.
#
#     # Sample code for loading in the sample json
#     with open(RESOURCES / "single_item.json") as f:
#         resource_json = json.load(f)
#
#     actual_data = ingester.get_record_data(resource_json)
#
#     expected_data = {
#         # TODO: Fill out the expected data which will be saved to the Catalog
#     }
#
#     assert actual_data == expected_data

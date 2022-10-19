# import json
from pathlib import Path

# import pytest
from providers.provider_api_scripts.smithsonian import SmithsonianDataIngester


RESOURCES = Path(__file__).parent / "tests/resources/smithsonian"

# Set up test class
ingester = SmithsonianDataIngester()


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

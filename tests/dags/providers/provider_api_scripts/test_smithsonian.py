# import json
from pathlib import Path
from unittest.mock import patch

import pytest
from airflow.exceptions import AirflowException
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


@pytest.mark.parametrize(
    "new_unit_codes, outdated_unit_codes",
    [
        ({"d"}, {"e"}),
        ({"d"}, set()),
        (set(), {"e"}),
    ],
)
def test_validate_unit_codes_from_api_raises_exception(
    new_unit_codes, outdated_unit_codes
):
    with patch.object(ingester, "_get_unit_codes_from_api"), patch.object(
        ingester,
        "_get_new_and_outdated_unit_codes",
        return_value=(new_unit_codes, outdated_unit_codes),
    ):
        message = "^\n\\*Updates needed to the SMITHSONIAN_SUB_PROVIDERS dictionary\\**"
        with pytest.raises(AirflowException, match=message):
            ingester.validate_unit_codes_from_api()


def test_validate_unit_codes_from_api():
    with patch.object(ingester, "_get_unit_codes_from_api"), patch.object(
        ingester, "_get_new_and_outdated_unit_codes", return_value=(set(), set())
    ):
        # Validation should run without raising an exception
        ingester.validate_unit_codes_from_api()


@pytest.mark.parametrize(
    "input_int, expect_len, expect_first, expect_last",
    [
        (1, 16, "0", "f"),
        (2, 256, "00", "ff"),
        (3, 4096, "000", "fff"),
        (4, 65536, "0000", "ffff"),
    ],
)
def test_get_hash_prefixes_with_other_len(
    input_int, expect_len, expect_first, expect_last
):
    with patch.object(ingester, "hash_prefix_length", input_int):
        actual_list = list(ingester._get_hash_prefixes())

    assert all("0x" not in h for h in actual_list)
    assert all(
        int(actual_list[i + 1], 16) - int(actual_list[i], 16) == 1
        for i in range(len(actual_list) - 1)
    )
    assert len(actual_list) == expect_len
    assert actual_list[0] == expect_first
    assert actual_list[-1] == expect_last


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


@pytest.mark.parametrize(
    "input_dnr, expect_image_list",
    [
        ({}, []),
        ({"non_media": {"media": ["image1", "image2"]}}, []),
        ({"online_media": "wrong type"}, []),
        ({"online_media": ["wrong", "type"]}, []),
        ({"online_media": {"media": "wrong type"}}, []),
        ({"online_media": {"media": {"wrong": "type"}}}, []),
        (
            {
                "record_ID": "siris_arc_291918",
                "online_media": {"mediaCount": 1, "media": ["image1", "image2"]},
            },
            ["image1", "image2"],
        ),
    ],
)
def test_get_image_list(input_dnr, expect_image_list):
    input_row = {"key": "val"}
    with patch.object(
        ingester, "_get_descriptive_non_repeating_dict", return_value=input_dnr
    ) as mock_dnr:
        actual_image_list = ingester._get_image_list(input_row)
    mock_dnr.assert_called_once_with(input_row)
    assert actual_image_list == expect_image_list


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

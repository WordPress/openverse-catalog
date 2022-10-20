# import json
from pathlib import Path
from unittest.mock import patch

import pytest
from airflow.exceptions import AirflowException
from common.licenses import get_license_info
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


@pytest.mark.parametrize(
    "input_dnr, expect_foreign_landing_url",
    [
        ({}, None),
        (
            {
                "guid": "http://fallback.com",
                "unit_code": "NMNHMAMMALS",
                "record_link": "http://chooseme.com",
            },
            "http://chooseme.com",
        ),
        ({"guid": "http://fallback.com"}, "http://fallback.com"),
        ({"no": "urlhere"}, None),
    ],
)
def test_get_foreign_landing_url(input_dnr, expect_foreign_landing_url):
    input_row = {"key": "val"}
    with patch.object(
        ingester, "_get_descriptive_non_repeating_dict", return_value=input_dnr
    ) as mock_dnr:
        actual_foreign_landing_url = ingester._get_foreign_landing_url(input_row)
    mock_dnr.assert_called_once_with(input_row)
    assert actual_foreign_landing_url == expect_foreign_landing_url


@pytest.mark.parametrize(
    "input_is, input_ft, expect_creator",
    [
        ({}, {}, None),
        ({"name": ["Alice"]}, {}, None),
        ({"name": "alice"}, {}, None),
        ({"name": [{"type": ["personal", "main"], "nocontent": "Alice"}]}, {}, None),
        ({"name": [{"type": "personal_main", "nocontent": "Alice"}]}, {}, None),
        ({"noname": [{"type": "personal_main", "content": "Alice"}]}, {}, None),
        ({"name": [{"label": "personal_main", "content": "Alice"}]}, {}, None),
        ({"name": [{"type": "impersonal_main", "content": "Alice"}]}, {}, None),
        (
            {"name": [{"type": "personal_main", "content": "Alice"}]},
            {"name": "Bob"},
            "Alice",
        ),
        (
            {"name": [{"type": "personal_main", "content": "Alice"}]},
            {"name": ["Bob"]},
            "Alice",
        ),
        (
            {"name": [{"type": "personal_main", "content": "Alice"}]},
            {"name": [{"label": "Creator", "nocontent": "Bob"}]},
            "Alice",
        ),
        (
            {"name": [{"type": "personal_main", "content": "Alice"}]},
            {"name": [{"nolabel": "Creator", "content": "Bob"}]},
            "Alice",
        ),
        (
            {"name": [{"type": "personal_main", "content": "Alice"}]},
            {"name": [{"label": "NotaCreator", "content": "Bob"}]},
            "Alice",
        ),
        (
            {"name": [{"type": "personal_main", "content": "Alice"}]},
            {"noname": [{"label": "Creator", "content": "Bob"}]},
            "Alice",
        ),
        (
            {"name": [{"type": "personal_main", "content": "Alice"}]},
            {"name": [{"label": "Creator", "content": "Bob"}]},
            "Bob",
        ),
        (
            {},
            {
                "name": [
                    {"label": "Designer", "content": "Alice"},
                    {"label": "Creator", "content": "Bob"},
                ]
            },
            "Bob",
        ),
        (
            {},
            {
                "name": [
                    {"label": "AFTER", "content": "Bob"},
                    {"label": "Designer", "content": "Alice"},
                ]
            },
            "Alice",
        ),
        (
            {},
            {
                "name": [
                    {"label": "AFTER", "content": "Bob"},
                    {"label": "DESIGNER", "content": "Alice"},
                ]
            },
            "Alice",
        ),
        (
            {
                "name": [
                    {"type": "personal_main", "content": "Alice"},
                    {"type": "corporate_subj", "content": "Zoological Park"},
                ]
            },
            {
                "name": [
                    {"label": "Creator", "content": "Bob"},
                    {"label": "Subject", "content": "Zoological Park"},
                ]
            },
            "Bob",
        ),
    ],
)
def test_get_creator(input_is, input_ft, expect_creator):
    creator_types = {"creator": 0, "designer": 1, "after": 3}
    input_row = {"test": "row"}
    get_is = patch.object(
        ingester, "_get_indexed_structured_dict", return_value=input_is
    )
    get_ft = patch.object(ingester, "_get_freetext_dict", return_value=input_ft)
    ct_patch = patch.object(ingester, "creator_types", creator_types)
    with get_is as mock_is, get_ft as mock_ft, ct_patch:
        actual_creator = ingester._get_creator(input_row)

    mock_is.assert_called_once_with(input_row)
    mock_ft.assert_called_once_with(input_row)
    assert actual_creator == expect_creator


@pytest.mark.parametrize(
    "input_ft, input_dnr, expect_description",
    [
        ({}, {}, None),
        ({"notes": [{"label": "notthis", "content": "blah"}]}, {}, None),
        ({"notes": "notalist"}, {}, None),
        ({"notes": [{"label": "Summary", "content": "blah"}]}, {}, "blah"),
        (
            {
                "notes": [
                    {"label": "Description", "content": "blah"},
                    {"label": "Summary", "content": "blah"},
                    {"label": "Description", "content": "blah"},
                ]
            },
            {},
            "blah blah blah",
        ),
        (
            {
                "notes": [
                    {"label": "notDescription", "content": "blah"},
                    {"label": "Summary", "content": "blah"},
                    {"label": "Description", "content": "blah"},
                ]
            },
            {},
            "blah blah",
        ),
    ],
)
def test_ext_meta_data_description(input_ft, input_dnr, expect_description):
    description_types = {"description", "summary"}
    input_row = {"test": "row"}
    get_dnr = patch.object(
        ingester, "_get_descriptive_non_repeating_dict", return_value=input_dnr
    )
    get_ft = patch.object(ingester, "_get_freetext_dict", return_value=input_ft)
    dt_patch = patch.object(ingester, "description_types", description_types)
    with get_dnr as mock_dnr, get_ft as mock_ft, dt_patch:
        meta_data = ingester._extract_meta_data(input_row)
    actual_description = meta_data.get("description")
    mock_dnr.assert_called_once_with(input_row)
    mock_ft.assert_called_once_with(input_row)
    assert actual_description == expect_description


@pytest.mark.parametrize(
    "input_ft, input_dnr, expect_label_text",
    [
        ({}, {}, None),
        ({"notes": [{"label": "notthis", "content": "blah"}]}, {}, None),
        ({"notes": "notalist"}, {}, None),
        ({"notes": [{"label": "Label Text", "content": "blah"}]}, {}, "blah"),
        (
            {
                "notes": [
                    {"label": "Label Text", "content": "blah"},
                    {"label": "Summary", "content": "halb"},
                    {"label": "Description", "content": "halb"},
                ]
            },
            {},
            "blah",
        ),
    ],
)
def test_ext_meta_data_label_text(input_ft, input_dnr, expect_label_text):
    input_row = {"test": "row"}
    get_dnr = patch.object(
        ingester, "_get_descriptive_non_repeating_dict", return_value=input_dnr
    )
    get_ft = patch.object(ingester, "_get_freetext_dict", return_value=input_ft)
    with get_dnr as mock_dnr, get_ft as mock_ft:
        meta_data = ingester._extract_meta_data(input_row)
    actual_label_text = meta_data.get("label_text")
    mock_dnr.assert_called_once_with(input_row)
    mock_ft.assert_called_once_with(input_row)
    assert actual_label_text == expect_label_text


@pytest.mark.parametrize(
    "input_ft, input_dnr, expect_meta_data",
    [
        ({"nothing": "here"}, {"nothing_to": "see"}, {}),
        ({}, {"unit_code": "SIA"}, {"unit_code": "SIA"}),
        (
            {},
            {"data_source": "Smithsonian Institution Archives"},
            {"data_source": "Smithsonian Institution Archives"},
        ),
    ],
)
def test_extract_meta_data_dnr_fields(input_ft, input_dnr, expect_meta_data):
    input_row = {"test": "row"}
    get_dnr = patch.object(
        ingester, "_get_descriptive_non_repeating_dict", return_value=input_dnr
    )
    get_ft = patch.object(ingester, "_get_freetext_dict", return_value=input_ft)
    with get_dnr as mock_dnr, get_ft as mock_ft:
        actual_meta_data = ingester._extract_meta_data(input_row)
    mock_dnr.assert_called_once_with(input_row)
    mock_ft.assert_called_once_with(input_row)
    assert actual_meta_data == expect_meta_data


@pytest.mark.parametrize(
    "input_is, expect_tags",
    [
        ({}, []),
        ({"nothing": "here"}, []),
        (
            {
                "date": ["", ""],
                "place": ["Indian Ocean"],
            },
            ["Indian Ocean"],
        ),
        (
            {
                "date": ["2000s"],
                "object_type": ["Holotypes", "Taxonomic type specimens"],
                "topic": ["Paleogeneral", "Protists"],
                "place": ["Indian Ocean"],
            },
            [
                "2000s",
                "Holotypes",
                "Taxonomic type specimens",
                "Paleogeneral",
                "Protists",
                "Indian Ocean",
            ],
        ),
    ],
)
def test_extract_tags(input_is, expect_tags):
    input_row = {"test": "row"}
    get_is = patch.object(
        ingester, "_get_indexed_structured_dict", return_value=input_is
    )
    with get_is as mock_is:
        actual_tags = ingester._extract_tags(input_row)
    mock_is.assert_called_once_with(input_row)
    assert actual_tags == expect_tags


@pytest.mark.parametrize(
    "input_media, expected_image_data",
    [
        ([], []),
        (
            [
                {
                    "thumbnail": "https://thumbnail.one",
                    "idsId": "id_one",
                    "usage": {"access": "CC0"},
                    "guid": "http://gu.id.one",
                    "type": "Images",
                    "content": "https://image.url.one",
                },
                {
                    "thumbnail": "https://thumbnail.two",
                    "idsId": "id_two",
                    "usage": {"access": "CC0"},
                    "guid": "http://gu.id.two",
                    "type": "Images",
                    "content": "https://image.url.two",
                },
            ],
            [
                {
                    "foreign_identifier": "id_one",
                    "image_url": "https://image.url.one",
                },
                {
                    "foreign_identifier": "id_two",
                    "image_url": "https://image.url.two",
                },
            ],
        ),
    ],
)
def test_process_image_list(input_media, expected_image_data):
    partial_image_data = {
        "foreign_landing_url": "https://foreignlanding.url",
        "title": "The Title",
        "license_info": get_license_info(
            license_url="https://creativecommons.org/publicdomain/zero/1.0/"
        ),
        "creator": "Alice",
        "meta_data": {"unit_code": "NMNHBOTANY"},
        "source": "smithsonian_national_museum_of_natural_history",
        "raw_tags": ["tag", "list"],
    }
    expected_result = [(img | partial_image_data) for img in expected_image_data]
    actual_image_data = ingester._process_image_list(input_media, partial_image_data)
    assert actual_image_data == expected_result


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

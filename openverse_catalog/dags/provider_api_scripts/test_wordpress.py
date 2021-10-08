import json
import logging
from pathlib import Path
from unittest.mock import patch

import requests
import wordpress as wp


RESOURCES = Path(__file__).parent / "tests/resources/wordpress"
SAMPLE_MEDIA_DATA = RESOURCES / "full_item.json"
SAMPLE_MEDIA_DETAILS = RESOURCES / "full_item_details.json"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s",
    level=logging.DEBUG,
)


def test_get_query_params_returns_defaults():
    expected_result = {
        "format": "json",
        "page": 1,
        "per_page": 100,
        "order": "desc",
        "orderby": "date",
    }
    actual_result = wp._get_query_params()
    assert actual_result == expected_result


def test_get_query_params_returns_defaults_with_given_page():
    expected_result = {
        "format": "json",
        "page": 3,
        "per_page": 100,
        "order": "desc",
        "orderby": "date",
    }
    actual_result = wp._get_query_params(page=3)
    assert actual_result == expected_result


def test_get_item_page_returns_correctly_with_none_response():
    expected_result = (None, 0)
    endpoint = "example.com"
    with patch.object(wp.delayed_requester, "get", return_value=None):
        actual_result = wp._get_item_page(endpoint)
    assert actual_result == expected_result


def test_get_item_page_returns_correctly_with_no_results():
    expected_result = (None, 0)
    endpoint = "example.com"
    with patch.object(wp.delayed_requester, "get", return_value=requests.Response()):
        actual_result = wp._get_item_page(endpoint)
    assert actual_result == expected_result


def test_process_resource_batch_with_tags():
    with open(RESOURCES / "orientations_batch.json") as f:
        batch_data = json.load(f)
    actual_result = wp._process_resource_batch("photo-orientations", batch_data)
    expected_result = {23: "landscape", 24: "portrait", 25: "square"}
    assert actual_result == expected_result


def test_process_resource_batch_with_users():
    with open(RESOURCES / "users_batch.json") as f:
        batch_data = json.load(f)
    actual_result = wp._process_resource_batch("users", batch_data)
    expected_result = {3606: {"name": "Scott Reilly", "url": "http://coffee2code.com"}}
    assert actual_result == expected_result


# def test_get_items():
#     with open(RESOURCES / 'page1.json') as f:
#         first_response = json.load(f)
#     with patch.object(
#             wp,
#             '_get_batch_json',
#             side_effect=[first_response, []]
#     ):
#         expected_image_count = 3
#         actual_image_count = wp._get_items()
#         assert expected_image_count == actual_image_count


# def test_process_item_batch_handles_example_batch():
#     with open(SAMPLE_MEDIA_DATA) as f:
#         items_batch = [json.load(f)]
#     with patch.object(
#             wp.image_store,
#             'add_item',
#             return_value=1
#     ) as mock_add:
#         wp._process_item_batch(items_batch)
#         mock_add.assert_called_once()
#         _, actual_call_args = mock_add.call_args_list[0]
#         expected_call_args = {
#         }
#         assert actual_call_args == expected_call_args


def test_extract_image_data_returns_none_when_media_data_none():
    actual_image_info = wp._extract_image_data(None)
    expected_image_info = None
    assert actual_image_info is expected_image_info


def test_extract_image_data_returns_none_when_no_foreign_id():
    with open(SAMPLE_MEDIA_DATA) as f:
        image_data = json.load(f)
        image_data.pop("slug", None)
    actual_image_info = wp._extract_image_data(image_data)
    expected_image_info = None
    assert actual_image_info is expected_image_info


def test_extract_image_data_returns_none_when_no_image_details():
    with open(SAMPLE_MEDIA_DATA) as f:
        image_data = json.load(f)
        image_data.pop("_links", None)
    actual_image_info = wp._extract_image_data(image_data)
    assert actual_image_info is None


def test_get_title():
    with open(SAMPLE_MEDIA_DATA) as f:
        image_data = json.load(f)
    actual_result = wp._get_title(image_data)
    expected_result = "Lupinus polyphyllus (aka Washington lupine)"
    assert actual_result == expected_result


def test_get_file_info():
    with open(SAMPLE_MEDIA_DETAILS) as f:
        image_details = json.load(f)
    actual_result = wp._get_file_info(image_details)
    expected_result = (
        "https://pd.w.org/2021/06/56560bf1d69971f38.94814132.jpg",  # image_url
        4032,  # height
        3024,  # width
        "jpg",  # filetype
    )
    assert actual_result == expected_result


def test_get_creator_data():
    with open(SAMPLE_MEDIA_DATA) as f:
        image_data = json.load(f)
    image_related_patch = {
        "users": {
            3606: {"name": "Scott Reilly", "url": "http://coffee2code.com"},
        }
    }
    with patch.object(wp, "image_related", image_related_patch):
        actual_creator, actual_creator_url = wp._get_creator_data(image_data)
    expected_creator = "Scott Reilly"
    expected_creator_url = "http://coffee2code.com"

    assert actual_creator == expected_creator
    assert actual_creator_url == expected_creator_url


def test_get_creator_data_handle_no_author():
    with open(SAMPLE_MEDIA_DATA) as f:
        image_data = json.load(f)
    image_data.pop("author")
    image_related_patch = {
        "users": {
            3606: {"name": "Scott Reilly", "url": "http://coffee2code.com"},
        }
    }
    with patch.object(wp, "image_related", image_related_patch):
        actual_creator, actual_creator_url = wp._get_creator_data(image_data)
    assert actual_creator is None
    assert actual_creator_url is None


# def test_get_tags():
#     item_data = {
#         "tags": ['tag1', 'tag2']
#     }
#     expected_tags = ['tag1', 'tag2']
#     actual_tags = wp._get_tags(item_data)
#     assert expected_tags == actual_tags

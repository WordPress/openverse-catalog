# TODO: Test the small functions you created,
#  trying to find different edge cases (missing keys,
#  different data types returned, Nones, etc),
#  especially the ones found in  the `json` files
#  with API responses.


# Mock the functions that require internet access,
# such as url verification, so that the tests can run
# faster, and even offline.
import json
import logging
from pathlib import Path
from unittest.mock import patch

from providers.provider_api_scripts import inaturalist

RESOURCES = Path(__file__).parent / 'tests/resources/inaturalist'
SAMPLE_MEDIA_DATA = RESOURCES / 'image_data_example.json'

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.DEBUG,
)


def test_get_image_pages_returns_correctly_with_none_json():
    expect_result = None
    with patch.object(
            inaturalist.delayed_requester,
            'get_response_json',
            return_value=None
    ):
        actual_result = inaturalist._get_batch_json()
    assert actual_result == expect_result


def test_get_image_pages_returns_correctly_with_no_results():
    expect_result = None
    with patch.object(
            inaturalist.delayed_requester,
            'get_response_json',
            return_value={}
    ):
        actual_result = inaturalist._get_batch_json()
    assert actual_result == expect_result


def test_get_query_params_adds_offset():
    actual_qp = inaturalist._get_query_params(
        offset=200
    )
    expected_qp = {'offset': 200}
    assert actual_qp['offset'] == expected_qp['offset']


def test_get_query_params_leaves_other_keys():
    actual_qp = inaturalist._get_query_params(
        offset=200, default_query_params={'test': 'value'}
    )
    assert actual_qp['test'] == 'value'
    assert len(actual_qp.keys()) == 2


def test_get_items():
    with open(RESOURCES / 'page1.json') as f:
        first_response = json.load(f)
    with patch.object(
            inaturalist,
            '_get_batch_json',
            side_effect=[first_response, []]
    ):
        expected_image_count = 3
        actual_image_count = inaturalist._get_items()
        assert expected_image_count == actual_image_count


def test_process_item_batch_handles_example_batch():
    with open(SAMPLE_MEDIA_DATA) as f:
        items_batch = [json.load(f)]
    with patch.object(
            inaturalist.image_store,
            'add_item',
            return_value=1
    ) as mock_add:
        inaturalist._process_item_batch(items_batch)
        mock_add.assert_called_once()
        _, actual_call_args = mock_add.call_args_list[0]
        expected_call_args = {
        }
        assert actual_call_args == expected_call_args


def test_extract_image_data_returns_none_when_media_data_none():
    actual_image_info = inaturalist._extract_item_data(None)
    expected_image_info = None
    assert actual_image_info is expected_image_info


def test_extract_image_data_returns_none_when_no_foreign_id():
    with open(SAMPLE_MEDIA_DATA) as f:
        image_data = json.load(f)
        image_data.pop('foreign_id', None)
    actual_image_info = inaturalist._extract_item_data(image_data)
    expected_image_info = None
    assert actual_image_info is expected_image_info


def test_extract_image_data_returns_none_when_no_image_url():
    with open(SAMPLE_MEDIA_DATA) as f:
        image_data = json.load(f)
        image_data.pop('image_url', None)
    actual_image_info = inaturalist._extract_item_data(image_data)
    assert actual_image_info is None


def test_extract_image_data_returns_none_when_no_license():
    with open(SAMPLE_MEDIA_DATA) as f:
        image_data = json.load(f)
        image_data.pop('license_url', None)
    actual_image_info = inaturalist._extract_item_data(image_data)
    assert actual_image_info is None


def test_get_creator_data():
    with open(SAMPLE_MEDIA_DATA) as f:
        image_data = json.load(f)
    actual_creator, actual_creator_url = inaturalist._get_creator_data(image_data)
    expected_creator = ''
    expected_creator_url = ''

    assert actual_creator == expected_creator
    assert actual_creator_url == expected_creator_url


def test_get_creator_data_handles_no_url():
    with open(SAMPLE_MEDIA_DATA) as f:
        image_data = json.load(f)
    image_data.pop('artist_url', None)
    expected_creator = ''

    actual_creator, actual_creator_url = inaturalist._get_creator_data(image_data)
    assert actual_creator == expected_creator
    assert actual_creator_url is None


def test_get_creator_data_returns_none_when_no_artist():
    with open(SAMPLE_MEDIA_DATA) as f:
        image_data = json.load(f)
    image_data.pop('artist_name', None)
    actual_creator, actual_creator_url = inaturalist._get_creator_data(image_data)

    assert actual_creator is None
    assert actual_creator_url is None


def test_extract_image_data_handles_example_dict():
    with open(SAMPLE_MEDIA_DATA) as f:
        image_data = json.load(f)

    actual_image_info = inaturalist._extract_item_data(image_data)
    expected_image_info = {}
    assert actual_image_info == expected_image_info


def test_get_tags():
    item_data = {
        "tags": ['tag1', 'tag2']
    }
    expected_tags = ['tag1', 'tag2']
    actual_tags = inaturalist._get_tags(item_data)
    assert expected_tags == actual_tags

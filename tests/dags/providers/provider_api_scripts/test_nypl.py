import json
import logging
from pathlib import Path
from unittest.mock import patch

import pytest
from common.loader import provider_details as prov
from common.storage.image import ImageStore
from providers.provider_api_scripts.nypl import NyplDataIngester


RESOURCES = Path(__file__).parent / "resources/nypl"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.DEBUG
)


@pytest.fixture(autouse=True)
def validate_url_string():
    with patch("common.urls.validate_url_string") as mock_validate_url_string:
        mock_validate_url_string.side_effect = lambda x: x
        yield


nypl = NyplDataIngester()
image_store = ImageStore(provider=prov.NYPL_DEFAULT_PROVIDER)
nypl.media_stores = {"image": image_store}


def _get_resource_json(json_name):
    with open(RESOURCES / json_name) as f:
        resource_json = json.load(f)
        return resource_json


def test_get_next_query_params_default():
    actual_param = nypl.get_next_query_params({})
    expected_param = {"q": "CC_0", "field": "use_rtxt_s", "page": 1, "per_page": 500}
    assert actual_param == expected_param


def test_get_next_query_params_increments_offset():
    previous_query_params = {
        "q": "CC_0",
        "field": "use_rtxt_s",
        "page": 10,
        "per_page": 500,
    }

    actual_param = nypl.get_next_query_params(previous_query_params)
    expected_param = {"q": "CC_0", "field": "use_rtxt_s", "page": 11, "per_page": 500}
    assert actual_param == expected_param


def test_get_batch_data_success():
    response_search_success = _get_resource_json("response_search_success.json")
    actual_response = nypl.get_batch_data(response_search_success)

    assert len(actual_response) == 1


def test_get_batch_data_failure():
    response_search_failure = {}
    actual_response = nypl.get_batch_data(response_search_failure)

    assert actual_response is None


def test_get_creators_success():
    creatorinfo = _get_resource_json("creator_info_success.json")
    actual_creator = nypl._get_creators(creatorinfo)
    expected_creator = "Hillman, Barbara"

    assert actual_creator == expected_creator


def test_get_creators_failure():
    creatorinfo = []
    actual_creator = nypl._get_creators(creatorinfo)

    assert actual_creator is None


def test_get_metadata():
    item_response = _get_resource_json("response_itemdetails_success.json")
    mods = item_response.get("nyplAPI").get("response").get("mods")
    actual_metadata = nypl._get_metadata(mods)
    expected_metadata = _get_resource_json("metadata.json")

    assert actual_metadata == expected_metadata


def test_get_metadata_missing_attrs():
    item_response = _get_resource_json("response_itemdetails_success.json")
    mods = item_response.get("nyplAPI").get("response").get("mods")
    # Remove data to simulate it being missing
    mods["originInfo"].pop("dateIssued")
    mods["originInfo"].pop("publisher")
    mods["physicalDescription"].pop("note")
    # Remove data from expected values too
    expected_metadata = _get_resource_json("metadata.json")
    for attr in ["date_issued", "publisher", "physical_description"]:
        expected_metadata.pop(attr)

    actual_metadata = nypl._get_metadata(mods)

    assert actual_metadata == expected_metadata


def test_get_record_data_success():
    search_response = _get_resource_json("response_search_success.json")
    result = search_response["nyplAPI"]["response"]["result"][0]
    item_response = _get_resource_json("response_itemdetails_success.json")

    with patch.object(nypl, "get_detail_json", return_value=item_response):
        images = nypl.get_record_data(result)
    assert len(images) == 7


def test_get_record_data_failure():
    search_response = _get_resource_json("response_search_success.json")
    result = search_response["nyplAPI"]["response"]["result"][0]

    item_response = None
    with patch.object(nypl, "get_detail_json", return_value=item_response):
        images = nypl.get_record_data(result)
    assert images is None

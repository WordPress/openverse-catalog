import json
import logging
import os
from unittest.mock import patch

from common.licenses import get_license_info
from common.loader import provider_details as prov
from common.storage.image import ImageStore
from providers.provider_api_scripts.stocksnap import StockSnapDataIngester


RESOURCES = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), "resources/stocksnap"
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s",
    level=logging.DEBUG,
)


stocksnap = StockSnapDataIngester()
image_store = ImageStore(provider=prov.STOCKSNAP_DEFAULT_PROVIDER)
stocksnap.media_stores = {"image": image_store}


#     def _get_image_info(item):
#     def _get_title(item):
#     def _get_metadata(item):
# def ingest_records(self, **kwargs) -> None:
# def get_batch(self, query_params: Dict) -> Tuple[Optional[List], bool]:
# def get_should_continue(self, response_json):
# def get_batch_data(self, response_json):
# def process_batch(self, media_batch):


def _get_resource_json(json_name):
    with open(os.path.join(RESOURCES, json_name)) as f:
        resource_json = json.load(f)
        return resource_json


def test_get_media_type():
    expect_result = "image"
    actual_result = stocksnap.get_media_type(_get_resource_json("full_item.json"))
    assert expect_result == actual_result


def test_endpoint_with_initialized_page_counter():
    expect_result = "https://stocksnap.io/api/load-photos/date/desc/0"
    actual_result = stocksnap.endpoint
    assert expect_result == actual_result


def test_get_batch_data_returns_correctly_with_none_json():
    expect_result = None
    actual_result = stocksnap.get_batch_data(None)
    assert actual_result == expect_result


def test_get_batch_data_returns_correctly_with_no_results():
    expect_result = None
    actual_result = stocksnap.get_batch_data({})
    assert actual_result == expect_result


def test_get_batch_data_returns_correctly_with_full_response():
    actual_result = stocksnap.get_batch_data(_get_resource_json("full_response.json"))
    assert isinstance(actual_result, list)
    assert len(actual_result) == 40
    assert actual_result[0] == {
        "img_id": "OAJ3645ZWF",
        "tags": "flower,   blossom,   macro,   close up,   fresh,   petals,   flora,   floral,   plants,   organic,   natural,   spring,   bloom,   minimal,   background,   wallpaper,   texture,   pattern,   delicate,   detail,   botanical,  magnolia",
        "page_views": 25,
        "downloads": 5,
        "favorites": 0,
        "img_width": 6000,
        "img_height": 4000,
        "author_name": "Macro Mama",
        "author_id": 125683,
        "author_website": "https://stocksnap.io/",
        "author_profile": "https://stocksnap.io/author/125683",
        "adjustedWidth": 420,
        "page_views_raw": 25,
        "downloads_raw": 5,
        "favorites_raw": 0,
        "keywords": [
            "flower",
            "blossom",
            "macro",
            "closeup",
            "fresh",
            "petals",
            "flora",
            "floral",
            "plants",
            "organic",
            "natural",
            "spring",
            "bloom",
            "minimal",
            "background",
            "wallpaper",
            "texture",
            "pattern",
            "delicate",
            "detail",
            "botanical",
            "magnolia",
        ],
        "favorited": False,
    }


def test_process_batch():
    expect_result = 40
    actual_result = stocksnap.process_batch(
        stocksnap.get_batch_data(_get_resource_json('full_response.json'))
        )
    assert expect_result == actual_result


def test_endpoint_increment():
    expect_result = (None, "https://stocksnap.io/api/load-photos/date/desc/1")
    query_params = stocksnap.get_next_query_params(None)
    next_endpoint = stocksnap.endpoint
    actual_result = (query_params, next_endpoint)
    assert expect_result == actual_result


def test_get_record_data_returns_none_when_media_data_none():
    actual_image_info = stocksnap.get_record_data(None)
    expected_image_info = None
    assert actual_image_info is expected_image_info


def test_get_record_data_returns_none_when_no_foreign_id():
    image_data = _get_resource_json("full_item.json")
    image_data.pop("img_id", None)
    actual_image_info = stocksnap.get_record_data(image_data)
    expected_image_info = None
    assert actual_image_info is expected_image_info


def test_get_creator_data():
    img_data = _get_resource_json("full_item.json")
    expected_creator = "Matt Moloney"
    expected_creator_url = "https://mjmolo.com/"

    actual_creator, actual_creator_url = stocksnap._get_creator_data(img_data)
    assert actual_creator == expected_creator
    assert actual_creator_url == expected_creator_url


def test_get_creator_data_handles_no_url():
    img_data = _get_resource_json("full_item.json")
    img_data.pop("author_website")
    img_data.pop("author_profile")
    expected_creator = "Matt Moloney"

    actual_creator, actual_creator_url = stocksnap._get_creator_data(img_data)
    assert actual_creator == expected_creator
    assert actual_creator_url is None


def test_get_creator_data_returns_stocksnap_author_profile():
    img_data = _get_resource_json("full_item.json")
    img_data["author_website"] = "https://stocksnap.io/"
    expected_creator = "Matt Moloney"
    expected_creator_url = "https://stocksnap.io/author/111564"

    actual_creator, actual_creator_url = stocksnap._get_creator_data(img_data)
    assert actual_creator == expected_creator
    assert actual_creator_url == expected_creator_url


def test_get_creator_data_returns_none_when_no_author():
    img_data = _get_resource_json("full_item.json")
    img_data.pop("author_name")
    actual_creator, actual_creator_url = stocksnap._get_creator_data(img_data)
    assert actual_creator is None
    assert actual_creator_url is None


def test_get_record_data_handles_example_dict():
    with open(os.path.join(RESOURCES, "full_item.json")) as f:
        image_data = json.load(f)

    with patch.object(stocksnap, "_get_filesize", return_value=123456):
        actual_image_info = stocksnap.get_record_data(image_data)
    image_url = "https://cdn.stocksnap.io/img-thumbs/960w/7VAQUG1X3B.jpg"
    expected_image_info = {
        "title": "Female Fitness Photo",
        "creator": "Matt Moloney",
        "creator_url": "https://mjmolo.com/",
        "foreign_identifier": "7VAQUG1X3B",
        "foreign_landing_url": "https://stocksnap.io/photo/7VAQUG1X3B",
        "license_info": get_license_info(
            license_url="https://creativecommons.org/publicdomain/zero/1.0/"
        ),
        "image_url": image_url,
        "filesize": 123456,
        "filetype": "jpg",
        "height": 4000,
        "width": 6000,
        "meta_data": {
            "page_views_raw": 30,
            "downloads_raw": 0,
            "favorites_raw": 0,
        },
        "raw_tags": [
            "female",
            "fitness",
            "trainer",
            "model",
            "outdoors",
            "fit",
            "workout",
            "health",
            "woman",
            "field",
            "girl",
            "pose",
            "sport",
            "athlete",
            "recreation",
            "wellness",
            "people",
            "fashion",
            "day",
            "active",
            "sports",
            "track",
            "stretch",
            "lifestyle",
            "squat",
        ],
        "category": "photograph",
    }
    assert actual_image_info == expected_image_info


def test_get_image_tags():
    item_data = {
        "keywords": [
            "sunflowers",
            "nature",
            "flower",
        ],
    }
    expected_tags = ["sunflowers", "nature", "flower"]
    actual_tags = stocksnap._get_tags(item_data)
    assert expected_tags == actual_tags

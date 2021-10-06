"""
Content Provider:       Wordpress Photo Directory

ETL Process:            Use the API to identify all openly licensed media.

Output:                 TSV file containing the media metadata.

Notes:                  {{API URL}}
                        No rate limit specified.
"""
import json
import logging
from pathlib import Path
from urllib.parse import urlparse

import lxml.html as html
from common.licenses.licenses import get_license_info
from common.requester import DelayedRequester
from storage.image import ImageStore
from util.loader import provider_details as prov


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

LIMIT = 100  # number of items per page in API response
DELAY = 1  # in seconds
RETRIES = 3

# TODO: Update HOST and ENDPOINT when the actual API is ready
HOST = "photodir.zack.cat"
ENDPOINT = f"https://{HOST}/api/wp-json/wp/v2/photos"

PROVIDER = prov.WORDPRESS_DEFAULT_PROVIDER
# TODO: Add the API key to `openverse_catalog/env.template` if required
# API_KEY = os.getenv("WORDPRESS", "nokeyprovided")

# TODO: Add any headers necessary for API request
HEADERS = {
    "Accept": "application/json",
    # "api_key": API_KEY,
}

DEFAULT_QUERY_PARAMS = {
    "format": "json",
    "page": 1,
    "per_page": LIMIT,
    "order": "desc",
    "orderby": "date",
}

delayed_requester = DelayedRequester(DELAY)
image_store = ImageStore(provider=PROVIDER)

license_url = "https://creativecommons.org/publicdomain/zero/1.0/"
license_info = get_license_info(license_url=license_url)

saved_json_counter = {
    "full_response": 0,
    "empty_response": 0,
    "full_item": 0,
    "no_image_details": 0,
    "no_image_url": 0,
    "no_foreign_landing_url_or_id": 0,
    "no_license": 0,
}


def check_and_save_json_for_test(name, data):
    parent = Path(__file__).parent
    test_resources_path = parent / "tests" / "resources" / "wordpress"
    if not Path.is_dir(test_resources_path):
        Path.mkdir(test_resources_path)
    if saved_json_counter[name] == 0:
        with open(f"{name}.json", "w+", encoding="utf-8") as outf:
            json.dump(data, outf, indent=2)
        saved_json_counter[name] += 1


def main():
    """
    This script pulls the data from the Wordpress Photo Directory and writes it into a
    .TSV file to be eventually read into our DB.
    """

    logger.info("Begin: Wordpress Photo Directory script")
    image_count = _get_items()
    image_store.commit()
    logger.info(f"Total images pulled: {image_count}")
    logger.info("Terminated!")


def _get_query_params(page=1, default_query_params=None):
    if default_query_params is None:
        default_query_params = DEFAULT_QUERY_PARAMS
    query_params = default_query_params.copy()
    query_params["page"] = page
    return query_params


def _get_items():
    item_count, page = 0, 1
    should_continue = True
    while should_continue:
        query_params = _get_query_params(page=page)
        batch_data, total_pages = _get_image_pages(query_params=query_params)
        if isinstance(batch_data, list) and len(batch_data) > 0 and total_pages >= page:
            item_count = _process_item_batch(batch_data)
            page += 1
        else:
            should_continue = False
    return item_count


def _get_image_pages(
    endpoint=ENDPOINT, headers=None, retries=RETRIES, query_params=None
):
    response_json, total_pages = None, None
    if retries < 0:
        logger.error("No retries remaining. Returning Nonetypes.")
        return None, 0

    if headers is None:
        headers = HEADERS.copy()

    response = delayed_requester.get(endpoint, query_params, headers=headers)
    if response is not None and response.status_code == 200:
        try:
            response_json = response.json()
            total_pages = response.headers["X-WP-TotalPages"]
        except Exception as e:
            logger.warning(f"Response not captured due to {e}")
            response_json = None

    if response_json is None or total_pages is None:
        logger.warning(
            "Retrying:\n_get_batch_json(\n"
            f"    {endpoint},\n"
            f"    {query_params},\n"
            f"    retries={retries - 1}"
            ")"
        )
        response_json, total_pages = _get_image_pages(
            endpoint, headers, retries - 1, query_params
        )
    return response_json, total_pages


def _process_item_batch(items_batch):
    for item in items_batch:
        item_meta_data = _extract_item_data(item)
        if item_meta_data is None:
            continue
        image_store.add_item(**item_meta_data)
    return image_store.total_items


def _get_image_details(media_data):
    url = media_data.get("_links", {}).get("wp:featuredmedia", {}).get("href")
    if url is None:
        return None
    response_json = delayed_requester.get_response_json(url, RETRIES)
    return response_json


def _extract_item_data(media_data):
    """
    Extract data for individual item.
    """
    # TODO: remove the code for saving json files from the final script
    try:
        foreign_identifier = media_data["slug"]
        foreign_landing_url = media_data["link"]
    except (TypeError, KeyError, AttributeError):
        print("Found no foreign identifier or no foreign landing url:")
        print(json.dumps(media_data, indent=2))
        check_and_save_json_for_test("no_foreign_landing_url_or_id", media_data)
        return None
    title = _get_title(media_data)

    image_details = _get_image_details(media_data)
    if image_details is None:
        print("Found no image details:")
        print(json.dumps(media_data, indent=2))
        check_and_save_json_for_test("no_image_details", media_data)
        return None
    image_url, height, width, filetype = _get_file_info(image_details)
    if image_url is None:
        print("Found no image url:")
        print(json.dumps(media_data, indent=2))
        check_and_save_json_for_test("no_image_url", media_data)
        return None
    thumbnail = _get_thumbnail_url(image_details)
    metadata = _get_metadata(image_details)

    creator, creator_url = _get_creator_data(media_data)
    tags = _get_tags(media_data)
    check_and_save_json_for_test("full_item", media_data)

    return {
        "title": title,
        "creator": creator,
        "creator_url": creator_url,
        "foreign_identifier": foreign_identifier,
        "foreign_landing_url": foreign_landing_url,
        "image_url": image_url,
        "height": height,
        "width": width,
        "thumbnail_url": thumbnail,
        "filetype": filetype,
        "license_info": license_info,
        "meta_data": metadata,
        "raw_tags": tags,
    }


def _get_file_info(image_details):
    file_details = (
        image_details.get("media_details", {}).get("sizes", {}).get("full", {})
    )
    image_url = file_details.get("source_url")
    height = file_details.get("height")
    width = file_details.get("width")
    filetype = None
    if filename := file_details.get("file"):
        filetype = Path(filename).suffix.replace(".", "")
    return image_url, height, width, filetype


def _get_thumbnail_url(image_details):
    return (
        image_details.get("media_details", {})
        .get("sizes", {})
        .get("thumbnail", {})
        .get("source_url")
    )


def _get_creator_data(item):
    # TODO: Add correct implementation of _get_creator_data
    creator = item.get("author")
    creator_url = _cleanse_url(item.get("_links", {}).get("author", {}).get("href"))
    return creator, creator_url


def _get_title(item):
    title = item.get("content", {}).get("rendered")
    if title:
        title = html.fromstring(title).text_content()
    return title


def _get_metadata(image_details):
    raw_metadata = image_details.get("media_details", {}).get("image_meta", {})
    extras = [
        "aperture",
        "camera",
        "created_timestamp",
        "focal_length",
        "iso",
        "shutter_speed",
    ]
    metadata = {}
    for key in extras:
        value = raw_metadata.get(key)
        if value not in [None, ""]:
            metadata[key] = value

    if published_date := image_details.get("date"):
        metadata["published_date"] = published_date
    # TODO: include categories, colors, orientation
    return metadata


def _get_tags(item):
    # TODO: Add correct implementation of _get_tags
    return item.get("tags")


def _cleanse_url(url_string):
    """
    Check to make sure that a url is valid, and prepend a protocol if needed.
    """
    parse_result = urlparse(url_string)

    if parse_result.netloc == HOST:
        parse_result = urlparse(url_string, scheme="https")
    elif not parse_result.scheme:
        parse_result = urlparse(url_string, scheme="http")

    if parse_result.netloc or parse_result.path:
        return parse_result.geturl()


if __name__ == "__main__":
    main()

# TODO: Remove unnecessary comments
# TODO: Lint your code with pycodestyle

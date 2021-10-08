"""
Content Provider:       Wordpress Photo Directory

ETL Process:            Use the API to identify all openly licensed media.

Output:                 TSV file containing the media metadata.

Notes:                  {{API URL}}
                        No rate limit specified.
"""
import json
import logging

# import os
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

HOST = "wordpress.org"
ENDPOINT = f"http://{HOST}/photos/wp-json/wp/v2"

PROVIDER = prov.WORDPRESS_DEFAULT_PROVIDER
# TODO: Add the API key to `openverse_catalog/env.template` if required
# API_KEY = os.getenv("WORDPRESS", "nokeyprovided")
# USERNAME = os.getenv("WP_USERNAME")
# PASSWORD = os.getenv("WP_PASSWORD")

HEADERS = {
    "Accept": "application/json",
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

image_related = {
    "users": {},  # authors
    "photo-categories": {},
    "photo-colors": {},
    "photo-orientations": {},
    "photo-tags": {},
}

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
    _prefetch_image_related_data()
    image_count = _get_images()
    image_store.commit()
    logger.info(f"Total images pulled: {image_count}")
    logger.info("Terminated!")


def _prefetch_image_related_data():
    for resource in image_related.keys():
        collection = _get_resources(resource)
        image_related[resource] = collection


def _get_resources(resource_type):
    page = 1
    collection = {}
    endpoint = f"{ENDPOINT}/{resource_type}"
    should_continue = True
    while should_continue:
        query_params = _get_query_params(page=page)
        batch_data, total_pages = _get_item_page(endpoint, query_params=query_params)
        if isinstance(batch_data, list) and len(batch_data) > 0 and total_pages >= page:
            collection_page = _process_resource_batch(resource_type, batch_data)
            collection = collection | collection_page
            page += 1
        else:
            should_continue = False
    return collection


def _process_resource_batch(resource_type, batch_data):
    collected_page = {}
    item_id, name, url = None, None, None
    for item in batch_data:
        try:
            item_id = item["id"]
            name = item["name"]
            if resource_type == "users":
                # 'url' is the website of the author and 'link' would be their wp.org
                # profile, so at least the last must always be present
                url = _cleanse_url(item["url"] or item["link"])
        except Exception as e:
            logger.error(f"Couldn't save resource({resource_type}) info due to {e}")
            continue
        if resource_type == "users":
            collected_page[item_id] = {"name": name, "url": url}
        else:
            collected_page[item_id] = name
    return collected_page


def _get_query_params(page=1, default_query_params=None):
    if default_query_params is None:
        default_query_params = DEFAULT_QUERY_PARAMS
    query_params = default_query_params.copy()
    query_params["page"] = page
    return query_params


def _get_item_page(endpoint, retries=RETRIES, query_params=None):
    response_json, total_pages = None, None
    if retries < 0:
        logger.error("No retries remaining. Returning Nonetypes.")
        return None, 0

    response = delayed_requester.get(endpoint, query_params)
    if response is not None and response.status_code == 200:
        try:
            response_json = response.json()
            total_pages = response.headers["X-WP-TotalPages"]
        except Exception as e:
            logger.warning(f"Response not captured due to {e}")
            response_json = None

    if response_json is None or total_pages is None:
        logger.warning(
            "Retrying:\n_get_item_page(\n"
            f"    {endpoint},\n"
            f"    {query_params},\n"
            f"    retries={retries - 1}"
            ")"
        )
        response_json, total_pages = _get_item_page(endpoint, retries - 1, query_params)
    return response_json, total_pages


def _get_images():
    item_count, page = 0, 1
    endpoint = f"{ENDPOINT}/photos"
    should_continue = True
    while should_continue:
        query_params = _get_query_params(page=page)
        batch_data, total_pages = _get_item_page(endpoint, query_params=query_params)
        if isinstance(batch_data, list) and len(batch_data) > 0 and total_pages >= page:
            item_count = _process_image_batch(batch_data)
            page += 1
        else:
            should_continue = False
    return item_count


def _process_image_batch(image_batch):
    for item in image_batch:
        item_meta_data = _extract_image_data(item)
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


def _extract_image_data(media_data):
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
    # tags = _get_tags(media_data)
    # check_and_save_json_for_test("full_item", media_data)

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
        # "raw_tags": tags,
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
    creator, creator_url = None, None
    if author_id := item.get("author"):
        creator = image_related.get("users").get(author_id, {}).get("name")
        creator_url = image_related.get("users").get(author_id, {}).get("url")
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

import os
import logging
from urllib.parse import urlparse, parse_qs
from common.requester import DelayedRequester
from common.storage.image import ImageStore
from common.licenses.licenses import get_license_info
from util.loader import provider_details as prov

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

LIMIT = 500
DELAY = 1.0
RETRIES = 3
PROVIDER = prov.NYPL_DEFAULT_PROVIDER
BASE_ENDPOINT = "http://api.repo.nypl.org/api/v1/items/search"
METADATA_ENDPOINT = "http://api.repo.nypl.org/api/v1/items/item_details/"
NYPL_API = os.getenv("NYPL_API_KEY")
TOKEN = f"Token token={NYPL_API}"

delay_request = DelayedRequester(delay=DELAY)
image_store = ImageStore(provider=PROVIDER)

DEFAULT_QUERY_PARAM = {
    "q": "CC_0",
    "field": "use_rtxt_s",
    "page": 1,
    "per_page": LIMIT
}

HEADERS = {
    "Authorization": TOKEN
}

IMAGE_URL_DIMENSIONS = [
    "g", "v", "q", "w", "r"
]

THUMBNAIL_DIMENSIONS = [
    "w", "r", "q", "f", "v", "g"
]


def main():
    page = 1
    condition = True
    while condition:
        query_param = _get_query_param(
            page=page
        )
        request_response = _request_handler(
            params=query_param
        )
        results = request_response.get("result")
        if type(results) == list and len(results) > 0:
            _handle_results(results)
            logger.info(f"{image_store.total_images} images till now")
            page = page + 1
        else:
            condition = False
    image_store.commit()
    logger.info(f"total images {image_store.total_images}")


def _get_query_param(
        default_query_param=DEFAULT_QUERY_PARAM,
        page=1,
        ):
    query_param = default_query_param
    query_param["page"] = page
    return query_param


def _request_handler(
        endpoint=BASE_ENDPOINT,
        params=None,
        headers=HEADERS,
        retries=RETRIES
        ):
    results = None
    for retry in range(retries):
        response = delay_request.get(
            endpoint,
            params=params,
            headers=headers
        )
        if response.status_code == 200:
            try:
                response_json = response.json()
                response_json = response_json.get("nyplAPI")
                results = response_json.get("response")
                break

            except Exception as e:
                logger.warning(f"Request failed due to {e}")
                results = None
        else:
            results = None
    return results


def _handle_results(results):
    for item in results:
        uuid = item.get("uuid")

        item_details = _request_handler(
            endpoint=METADATA_ENDPOINT+uuid,
        )
        if item_details is None:
            continue

        mods = item_details.get("mods")
        title = _get_title(
            mods.get("titleInfo")
        )
        creator = _get_creators(
            mods.get("name")
        )
        metadata = _get_metadata(mods)

        captures = item_details.get("sibling_captures", {}).get("capture", [])
        if type(captures) is not list:
            captures = [captures]

        _get_capture_details(
            captures=captures,
            metadata=metadata,
            creator=creator,
            title=title
        )


def _get_capture_details(
        captures=[],
        metadata=None,
        creator=None,
        title=None
        ):
    for img in captures:
        image_id = img.get("imageID", {}).get("$")
        if image_id is None:
            continue
        image_url, thumbnail_url = _get_images(
            img.get("imageLinks", {}).get("imageLink", [])
        )
        foreign_landing_url = img.get("itemLink", {}).get("$")
        license_url = img.get("rightsStatementURI", {}).get("$")
        if (
            image_url is None or foreign_landing_url is None or
            license_url is None
        ):
            continue

        image_store.add_item(
            foreign_identifier=image_id,
            foreign_landing_url=foreign_landing_url,
            image_url=image_url,
            license_info=get_license_info(license_url=license_url),
            thumbnail_url=thumbnail_url,
            title=title,
            creator=creator,
            meta_data=metadata
        )


def _get_title(titleinfo):
    title = None
    if type(titleinfo) == list and len(titleinfo) > 0:
        title = titleinfo[0].get("title", {}).get("$")
    return title


def _get_creators(creatorinfo):
    if type(creatorinfo) == list:
        primary_creator = (
            info.get("namePart", {}).get("$")
            for info in creatorinfo if info.get("usage") == "primary"
        )
        creator = next(primary_creator, None)
    else:
        creator = None

    if creator is None:
        logger.warning("No primary creator found")

    return creator


def _get_images(
        images,
        image_url_dimensions=IMAGE_URL_DIMENSIONS,
        thumbnail_dimensions=THUMBNAIL_DIMENSIONS
        ):
    image_url, thumbnail_url = None, None
    image_type = {
        parse_qs(urlparse(img.get("$")).query)['t'][0]: img.get("$")
        for img in images
    }

    image_url = _get_preferred_image(image_type, image_url_dimensions)
    thumbnail_url = _get_preferred_image(image_type, thumbnail_dimensions)

    return image_url, thumbnail_url


def _get_preferred_image(image_type, dimension_list):
    preferred_image = (
        image_type.get(dimension).replace("&download=1", "")
        for dimension in dimension_list
        if dimension in image_type
    )

    return next(preferred_image, None)


def _get_metadata(mods):
    metadata = {}

    type_of_resource = mods.get("typeOfResource")
    if (
        type(type_of_resource) == list
        and (type_of_resource[0].get("usage") == "primary")
    ):
        metadata["type_of_resource"] = type_of_resource[0].get("$")

    if type(mods.get("genre")) == dict:
        metadata["genre"] = mods.get("genre").get("$")

    origin_info = mods.get("originInfo")
    try:
        metadata['date_issued'] = origin_info.get("dateIssued").get("$")
    except AttributeError as e:
        logger.warning(f"date_issued not found due to {e}")

    try:
        metadata["publisher"] = origin_info.get("publisher").get("$")
    except AttributeError as e:
        logger.warning(f"publisher not found due to {e}")

    physical_description = mods.get("physicalDescription")
    try:
        metadata["description"] = physical_description.get("note").get("$")
    except AttributeError as e:
        logger.warning(f"description not found, due to {e}")

    return metadata


if __name__ == "__main__":
    main()

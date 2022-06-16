import logging
from typing import Dict, Tuple

from common.licenses import get_license_info
from common.loader import provider_details as prov
from common.requester import DelayedRequester
from common.storage.image import ImageStore


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

LIMIT = 100
DELAY = 5.0
RETRIES = 3
PROVIDER = prov.VICTORIA_DEFAULT_PROVIDER
ENDPOINT = "https://collections.museumsvictoria.com.au/api/search"
LANDING_PAGE = "https://collections.museumsvictoria.com.au/"

delay_request = DelayedRequester(delay=DELAY)
image_store = ImageStore(provider=PROVIDER)

HEADERS = {"User-Agent": prov.UA_STRING, "Accept": "application/json"}

DEFAULT_QUERY_PARAMS = {
    "hasimages": "yes",
    "perpage": LIMIT,
    "imagelicence": "cc by",
    "page": 0,
}

LICENSE_LIST = [
    "cc by-nc-nd",
    "cc by",
    "public domain",
    "cc by-nc",
    "cc by-nc-sa",
    "cc by-sa",
]

RECORDS_IDS = []


def main():
    for license_ in LICENSE_LIST:
        logger.info(f"querying for license {license_}")
        condition = True
        page = 0
        while condition:
            query_params = _get_query_params(license_type=license_, page=page)
            results = _get_batch_objects(params=query_params)

            if type(results) == list:
                if len(results) > 0:
                    _handle_batch_objects(results)
                    page += 1
                else:
                    condition = False
            else:
                condition = False
    image_count = image_store.commit()
    logger.info(f"Total images {image_count}")


def _get_query_params(default_query_params=None, license_type="cc by", page=0):
    if default_query_params is None:
        default_query_params = DEFAULT_QUERY_PARAMS
    query_params = default_query_params.copy()
    query_params["imagelicence"] = license_type
    query_params["page"] = page
    return query_params


def _get_batch_objects(endpoint=ENDPOINT, params=None, headers=None, retries=RETRIES):
    if headers is None:
        headers = HEADERS.copy()
    data = None
    for retry in range(retries):
        response = delay_request.get(endpoint, params, headers=headers)
        try:
            response_json = response.json()
            if type(response_json) == list:
                data = response_json
                break
        except Exception:
            data = None
    return data


def _handle_batch_objects(objects, landing_page=LANDING_PAGE):
    image_count = 0
    for obj in objects:
        object_id = obj.get("id")
        if object_id in RECORDS_IDS:
            continue
        RECORDS_IDS.append(object_id)
        foreign_landing_url = landing_page + object_id
        media_data = obj.get("media")
        if media_data is None:
            continue
        image_data = _get_media_info(media_data)
        if len(image_data) == 0:
            continue
        meta_data = _get_metadata(obj)
        title = obj.get("displayTitle")
        for img in image_data:
            image_count = image_store.add_item(
                foreign_identifier=img.get("image_id"),
                foreign_landing_url=foreign_landing_url,
                image_url=img.get("image_url"),
                height=img.get("height"),
                width=img.get("width"),
                filesize=img.get("filesize"),
                license_info=img.get("license_info"),
                title=title,
                creator=img.get("creators"),
                meta_data=meta_data,
            )
    return image_count


def _get_media_info(media_data):
    image_data = []
    for media in media_data:
        media_type = media.get("type")
        if media_type == "image":
            image_id = media.get("id")
            license_url = _get_license_url(media)
            if image_id is None or license_url is None:
                continue
            image_url, height, width, filesize = _get_image_data(media)
            if image_url is None:
                continue
            creators = _get_creator(media)
            license_info = get_license_info(license_url=license_url)
            image_data.append(
                {
                    "image_id": image_id,
                    "image_url": image_url,
                    "height": height,
                    "width": width,
                    "filesize": filesize,
                    "license_info": license_info,
                    "creators": creators,
                }
            )
    return image_data


def _get_image_data(
    media: Dict,
) -> Tuple[str | None, int | None, int | None, int | None]:
    height, width, filesize = None, None, None
    media_data = {}
    for size in ["large", "medium", "small"]:
        if size in media:
            media_data = media[size]
            break

    image_url = media_data.get("uri")
    if image_url is not None:
        height = media_data.get("height")
        width = media_data.get("width")
        filesize = media_data.get("size")
    return image_url, height, width, filesize


def _get_license_url(media):
    license_url = None
    license_ = media.get("licence")
    if license_ is not None:
        uri = license_.get("uri")
        if "creativecommons" in uri:
            license_url = uri
    return license_url


def _get_metadata(obj):
    metadata = {
        "datemodified": obj.get("dateModified"),
        "category": obj.get("category"),
        "description": obj.get("physicalDescription"),
    }

    keywords = obj.get("keywords")
    if type(keywords) == list:
        metadata["keywords"] = ",".join(keywords)

    classifications = obj.get("classifications")
    if type(classifications) == list:
        metadata["classifications"] = ",".join(classifications)

    for key, value in metadata.items():
        if value is None:
            metadata.pop(key)

    return metadata


def _get_creator(media: Dict) -> str | None:
    creators = None
    if type(media.get("creators")) == list:
        creators = ",".join(media.get("creators"))
    return creators


if __name__ == "__main__":
    main()

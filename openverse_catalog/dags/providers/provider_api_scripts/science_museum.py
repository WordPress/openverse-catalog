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
PROVIDER = prov.SCIENCE_DEFAULT_PROVIDER
ENDPOINT = "https://collection.sciencemuseumgroup.org.uk/search/"

delayed_requester = DelayedRequester(delay=DELAY)
image_store = ImageStore(provider=PROVIDER)

HEADERS = {"Accept": "application/json"}

DEFAULT_QUERY_PARAMS = {
    "has_image": 1,
    "image_license": "CC",
    "page[size]": LIMIT,
    "page[number]": 0,
    "date[from]": 0,
    "date[to]": 1500,
}

YEAR_RANGE = [
    (0, 1500),
    (1500, 1750),
    (1750, 1825),
    (1825, 1850),
    (1850, 1875),
    (1875, 1900),
    (1900, 1915),
    (1915, 1940),
    (1940, 1965),
    (1965, 1990),
    (1990, 2020),
]

# global variable to keep track of records pulled
RECORD_IDS = []


def main():
    logger.info("Begin: Science Museum script")
    for year_range in YEAR_RANGE:
        logger.info(f"Running for years {year_range}")
        from_year, to_year = year_range
        image_count = _page_records(from_year=from_year, to_year=to_year)
        logger.info(f"Images pulled till now {image_count}")
    image_count = image_store.commit()
    logger.info(f"Total images pulled {image_count}")


def _page_records(from_year, to_year):
    image_count = 0
    page_number = 0
    condition = True
    while condition:
        query_param = _get_query_param(
            page_number=page_number, from_year=from_year, to_year=to_year
        )
        batch_data = _get_batch_objects(query_param=query_param)
        if type(batch_data) == list:
            if len(batch_data) > 0:
                image_count = _handle_object_data(batch_data)
                page_number += 1
            else:
                condition = False
        else:
            condition = False
    return image_count


def _get_query_param(
    page_number=0, from_year=0, to_year=1500, default_query_param=None
):
    if default_query_param is None:
        default_query_param = DEFAULT_QUERY_PARAMS
    query_param = default_query_param.copy()
    query_param["page[number]"] = page_number
    query_param["date[from]"] = from_year
    query_param["date[to]"] = to_year
    return query_param


def _get_batch_objects(
    endpoint=ENDPOINT, headers=None, retries=RETRIES, query_param=None
):
    if headers is None:
        headers = HEADERS.copy()
    data = None
    for retry in range(retries):
        response = delayed_requester.get(endpoint, query_param, headers=headers)
        try:
            response_json = response.json()
            if "data" in response_json.keys():
                data = response_json.get("data")
                break
        except Exception as e:
            logger.error(f"Failed to due to {e}")
    return data


def _handle_object_data(batch_data):
    image_count = 0
    for obj_ in batch_data:
        id_ = obj_.get("id")
        if id_ in RECORD_IDS:
            continue
        RECORD_IDS.append(id_)
        foreign_landing_url = obj_.get("links", {}).get("self")
        if foreign_landing_url is None:
            continue
        obj_attributes = obj_.get("attributes")
        if obj_attributes is None:
            continue
        title = obj_attributes.get("summary_title")
        creator = _get_creator_info(obj_attributes)
        metadata = _get_metadata(obj_attributes)
        multimedia = obj_attributes.get("multimedia")
        if multimedia is None:
            continue
        for image_data in multimedia:
            foreign_id = image_data.get("admin", {}).get("uid")
            if foreign_id is None:
                continue
            processed = image_data.get("processed")
            image_url, height, width, filetype = _get_image_info(processed)
            if image_url is None:
                continue
            source = image_data.get("source")
            license_version = _get_license_version(source)
            if license_version is None:
                continue
            license_, version = license_version.lower().split(" ")
            license_ = license_.replace("cc-", "")
            license_info = get_license_info(license_=license_, license_version=version)
            image_count = image_store.add_item(
                foreign_identifier=foreign_id,
                foreign_landing_url=foreign_landing_url,
                image_url=image_url,
                height=height,
                width=width,
                filetype=filetype,
                license_info=license_info,
                creator=creator,
                title=title,
                meta_data=metadata,
            )
    return image_count


def _get_creator_info(obj_attr):
    creator_info = None
    life_cycle = obj_attr.get("lifecycle")
    if life_cycle:
        creation = life_cycle.get("creation")
        if type(creation) == list:
            maker = creation[0].get("maker")
            if type(maker) == list:
                creator_info = maker[0].get("summary_title")
    return creator_info


def _get_image_info(
    processed: Dict,
) -> Tuple[str | None, int | None, int | None, str | None]:
    height, width, filetype = None, None, None
    image_data = processed.get("large")
    if image_data is None:
        image_data = processed.get("medium", {})

    image_url = check_url(image_data.get("location"))
    if image_url:
        filetype = image_data.get("format")
        height, width = _get_dimensions(image_data)
    return image_url, height, width, filetype


def check_url(image_url: str | None) -> str | None:
    if not image_url:
        return None
    if image_url.startswith("http"):
        return image_url
    else:
        return f"https://coimages.sciencemuseumgroup.org.uk/images/{image_url}"


def _get_dimensions(image_data: Dict) -> Tuple[int | None, int | None]:
    """
    Returns the height and width of the image from a list of dictionaries
    with keys of "dimension", "units", "value".
    """
    size = {}
    dimensions = image_data.get("measurements", {}).get("dimensions")
    if dimensions:
        for dim in dimensions:
            size[dim.get("dimension")] = (
                dim.get("value") if dim.get("units") == "pixels" else None
            )
    return size.get("height"), size.get("width")


def _get_license_version(source: Dict | None) -> str | None:
    license_version = None
    if source:
        rights = source.get("legal", {}).get("rights")
        if type(rights) == list and len(rights) > 0:
            license_version = rights[0].get("usage_terms")
    return license_version


def _get_metadata(obj_attr):
    metadata = {}
    identifier = obj_attr.get("identifier")
    if type(identifier) == list:
        metadata["accession number"] = identifier[0].get("value")
    name = obj_attr.get("name")
    if type(name) == list:
        metadata["name"] = name[0].get("value")
    category = obj_attr.get("categories")
    if type(category) == list:
        metadata["category"] = category[0].get("value")
    creditline = obj_attr.get("legal")
    if type(creditline) == dict:
        metadata["creditline"] = creditline.get("credit_line")
    description = obj_attr.get("description")
    if type(description) == list:
        metadata["description"] = description[0].get("value")
    return metadata


if __name__ == "__main__":
    main()

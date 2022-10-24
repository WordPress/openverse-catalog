"""
Content Provider:       PhyloPic

ETL Process:            Use the API to identify all CC licensed images.

Output:                 TSV file containing the image,
                        their respective meta-data.

Notes:                  http://phylopic.org/api/
                        No rate limit specified.
"""

import argparse
import logging
from datetime import date, timedelta
from typing import Dict

from common import constants
from common.licenses import get_license_info
from common.loader import provider_details as prov
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


logger = logging.getLogger(__name__)


class PhylopicDataIngester(ProviderDataIngester):
    delay = 5
    endpoint = f"http://phylopic.org/api/a/image"
    providers = {"image": prov.PHYLOPIC_DEFAULT_PROVIDER}
    batch_limit = 25
    # Default number of days to process if date_end is not defined
    default_process_days = 7

    def __init__(self, *args):
        super().__init__(*args)
        # This is made an instance attribute rather than passed around via query params
        # because it actually comprises the URL we hit (not the params) depending on
        # whether we're running a dated DAG or not.
        self.offset = 0

    @staticmethod
    def _compute_date_range(date_start: str, days: int = default_process_days) -> str:
        """
        Given an ISO formatted date string and a number of days, compute the
        ISO string that represents the start date plus the days provided.
        """
        date_end = date.fromisoformat(date_start) + timedelta(days=days)
        return date_end.isoformat()

    def get_media_type(self, record: dict) -> str:
        return constants.IMAGE

    def get_response_json(
        self, query_params: dict, endpoint: str | None = None, **kwargs
    ):
        """
        Override for the upstream get_response_json function.

        Due to how the Phylopic API works, the crucial parameters are actually
        specified in the endpoint itself. We pop the endpoint off here and hand it
        to the parent function if it exists. We copy the parameter set before doing this
        because it may be the only value in the params, but we still want execution to
        continue, so we need a truthy value for prev_query_params.
        """
        params = query_params.copy()
        endpoint = params.pop("endpoint")
        super().get_response_json(query_params, endpoint, **kwargs)

    def get_next_query_params(self, prev_query_params: dict | None, **kwargs) -> dict:
        """
        Additional optional arguments:

        default_process_days: If a date is supplied to the ingester in the form of
        YYYY-MM-DD, this is the number of days from that date to process. By default,
        this DAG runs weekly and so default_process_days is 7.

        If this is being run as a dated DAG, **only one request is ever issued** to
        retrieve all updated IDs. As such, the dated version will only return one
        query param set whenever called. The full run DAG does require the typical
        offset + limit. All of these values are encoded in an "endpoint" field, which
        is used instead of query parameters when actually issuing the request.
        """
        base_endpoint = f"{self.endpoint}/list"
        if self.date:
            default_process_days = kwargs.get(
                "default_process_days", self.default_process_days
            )
            end_date = self._compute_date_range(self.date, default_process_days)
            return {
                # Get a list of objects uploaded/updated within a date range
                # http://phylopic.org/api/#method-image-time-range
                "endpoint": f"{base_endpoint}/modified/{self.date}/{end_date}"
            }

        # This code path is only reached for non-dated DAG runs
        if prev_query_params:
            # Update our offset based on the batch limit
            self.offset += self.batch_limit
        return {
            # Get all images and limit the results for each request.
            "endpoint": f"{base_endpoint}/{self.offset}/{self.batch_limit}"
        }

    def get_batch_data(self, response_json):
        """
        Process the returned IDs.

        The Phylopic API returns only lists of IDs in the initial request. We must take
        this request and iterate through all the IDs to get the metadata for each one.
        """
        data = []
        if response_json and response_json.get("success") is True:
            data = list(response_json.get("result"))

        if not data:
            logger.warning("No content available!")
            return None

        return data

    def get_record_data(self, data: dict) -> dict | list[dict] | None:
        id_ = data.get("uid")


def main(date_start: str = "all", date_end: str = None):
    """
    This script pulls the data for a given date from the PhyloPic
    API, and writes it into a .TSV file to be eventually read
    into our DB.

    Required Arguments:

    date_start:  Date String in the form YYYY-MM-DD. This date defines the beginning
                 of the range that the script will pull data from.
    date_end:    Date String in the form YYYY-MM-DD. Similar to `date_start`, this
                 defines the end of the range of data pulled. Defaults to
                 DEFAULT_PROCESS_DAYS (7) if undefined.
    """

    offset = 0

    logger.info("Begin: PhyloPic API requests")

    if date_start == "all":
        logger.info("Processing all images")
        param = {"offset": offset}

        image_count = _get_total_images()
        logger.info(f"Total images: {image_count}")

        while offset <= image_count:
            _add_data_to_buffer(**param)
            offset += LIMIT
            param = {"offset": offset}

    else:
        if date_end is None:
            date_end = _compute_date_range(date_start)
        param = {"date_start": date_start, "date_end": date_end}

        logger.info(f"Processing from {date_start} to {date_end}")
        _add_data_to_buffer(**param)

    image_store.commit()

    logger.info("Terminated!")


def _add_data_to_buffer(**kwargs):
    endpoint = _create_endpoint_for_IDs(**kwargs)
    IDs = _get_image_IDs(endpoint)
    if IDs is None:
        return

    for id_ in IDs:
        item_data = _get_object_json(id_)
        if item_data:
            _process_item(item_data)


def _process_item(item_data: Dict) -> None:
    details = _get_meta_data(item_data)
    if details is not None:
        image_store.add_item(**details)


def _get_total_images() -> int:
    # Get the total number of PhyloPic images
    total = 0
    endpoint = "http://phylopic.org/api/a/image/count"
    result = delayed_requester.get_response_json(endpoint, retries=2)

    if result and result.get("success") is True:
        total = result.get("result", 0)

    return total


def _create_endpoint_for_IDs(**kwargs):
    limit = LIMIT

    if ((date_start := kwargs.get("date_start")) is not None) and (
        (date_end := kwargs.get("date_end")) is not None
    ):
        endpoint = (
            f"http://phylopic.org/api/a/image/list/modified/{date_start}/{date_end}"
        )

    elif (offset := kwargs.get("offset")) is not None:
        # Get all images and limit the results for each request.
        endpoint = f"http://phylopic.org/api/a/image/list/{offset}/{limit}"

    else:
        raise ValueError("No valid selection criteria found!")
    return endpoint


def _get_image_IDs(_endpoint) -> list | None:
    result = delayed_requester.get_response_json(_endpoint, retries=2)
    image_IDs = []

    if result and result.get("success") is True:
        data = list(result.get("result"))

        if len(data) > 0:
            for item in data:
                image_IDs.append(item.get("uid"))

    if not image_IDs:
        logger.warning("No content available!")
        return None

    return image_IDs


def _get_object_json(_uuid: str) -> Dict | None:
    logger.info(f"Processing UUID: {_uuid}")

    endpoint = (
        f"http://phylopic.org/api/a/image/{_uuid}?options=credit+"
        "licenseURL+pngFiles+submitted+submitter+taxa+canonicalName"
        "+string+firstName+lastName"
    )
    result = None
    request = delayed_requester.get_response_json(endpoint, retries=2)
    if request and request.get("success") is True:
        result = request["result"]
    return result


def _get_meta_data(result: Dict) -> Dict:
    base_url = "http://phylopic.org"
    meta_data = {}
    _uuid = result.get("uid")
    license_url = result.get("licenseURL")

    meta_data["taxa"], title = _get_taxa_details(result)

    foreign_url = f"{base_url}/image/{_uuid}"

    (creator, meta_data["credit_line"], meta_data["pub_date"]) = _get_creator_details(
        result
    )

    img_url, width, height = _get_image_info(result, _uuid)

    if img_url is None:
        return None

    details = {
        "foreign_identifier": _uuid,
        "foreign_landing_url": foreign_url,
        "image_url": img_url,
        "license_info": get_license_info(license_url=license_url),
        "width": width,
        "height": height,
        "creator": creator,
        "title": title,
        "meta_data": meta_data,
    }
    return details


def _get_creator_details(result):
    credit_line = None
    pub_date = None
    creator = None
    first_name = result.get("submitter", {}).get("firstName")
    last_name = result.get("submitter", {}).get("lastName")
    if first_name and last_name:
        creator = f"{first_name} {last_name}".strip()

    if result.get("credit"):
        credit_line = result.get("credit").strip()
        pub_date = result.get("submitted").strip()

    return creator, credit_line, pub_date


def _get_taxa_details(result):
    taxa = result.get("taxa", [])
    # [0].get('canonicalName', {}).get('string')
    taxa_list = None
    title = ""
    if taxa:
        taxa = [
            _.get("canonicalName") for _ in taxa if _.get("canonicalName") is not None
        ]
        taxa_list = [_.get("string", "") for _ in taxa]

    if taxa_list:
        title = taxa_list[0]

    return taxa_list, title


def _get_image_info(result, _uuid):
    base_url = "http://phylopic.org"
    img_url = None
    width = None
    height = None

    image_info = result.get("pngFiles")
    if image_info:
        images = list(
            filter(lambda x: (int(str(x.get("width", "0"))) >= 257), image_info)
        )
        if len(images) > 0:
            image = sorted(images, key=lambda x: x["width"], reverse=True)[0]
            img_url = image.get("url")
            if not img_url:
                logging.warning(f"Image not detected in url: {base_url}/image/{_uuid}")
            else:
                img_url = f"{base_url}{img_url}"
                width = image.get("width")
                height = image.get("height")

    return img_url, width, height


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PhyloPic API Job", add_help=True)
    parser.add_argument(
        "--date-start",
        default="all",
        help="Identify all images starting from a particular date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--date-end",
        default=None,
        help="Used in conjunction with --date-start, identify all images ending on "
        f"a particular date (YYYY-MM-DD), defaults to {DEFAULT_PROCESS_DAYS} "
        "if not defined.",
    )

    args = parser.parse_args()

    main(args.date_start, args.date_end)

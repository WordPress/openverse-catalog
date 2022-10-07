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

from common import constants
from common.licenses import get_license_info
from common.loader import provider_details as prov
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


logger = logging.getLogger(__name__)


class PhylopicDataIngester(ProviderDataIngester):
    delay = 5
    host = "http://phylopic.org"
    # Use "base_endpoint" since class's "endpoint" parameter gets defined as a property
    base_endpoint = f"{host}/api/a/image"
    providers = {"image": prov.PHYLOPIC_DEFAULT_PROVIDER}
    batch_limit = 25
    # If a date is supplied to the ingester in the form of YYYY-MM-DD, this is the
    # number of days from that date to process. By default,this DAG runs weekly
    # and so default_process_days is 7.
    default_process_days = 7

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # This is made an instance attribute rather than passed around via query params
        # because the URL we hit includes it in the path (not the params) depending on
        # whether we're running a dated DAG or not. This needs to start at 0 after
        # getting called once, so we do not set it as a sentinel for the
        # get_next_query_params function.
        self.offset = None

    @staticmethod
    def _compute_date_range(date_start: str, days: int) -> str:
        """
        Given an ISO formatted date string and a number of days, compute the
        ISO string that represents the start date plus the days provided.
        """
        date_end = date.fromisoformat(date_start) + timedelta(days=days)
        return date_end.isoformat()

    def get_media_type(self, record: dict) -> str:
        return constants.IMAGE

    @property
    def endpoint(self) -> str:
        """
        If this is being run as a dated DAG, **only one request is ever issued** to
        retrieve all updated IDs. As such, the dated version will only return one
        endpoint. The full run DAG does require the typical offset + limit, which gets
        recomputed on each call to this property.
        """
        list_endpoint = f"{self.base_endpoint}/list"
        if self.date:
            end_date = self._compute_date_range(self.date, self.default_process_days)
            # Get a list of objects uploaded/updated within a date range
            # http://phylopic.org/api/#method-image-time-range
            endpoint = f"{list_endpoint}/modified/{self.date}/{end_date}"
        else:
            # Get all images and limit the results for each request.
            endpoint = f"{list_endpoint}/{self.offset}/{self.batch_limit}"
        logger.info(f"Constructed endpoint: {endpoint}")
        return endpoint

    def get_next_query_params(self, prev_query_params: dict | None, **kwargs) -> dict:
        """
        Since the query range is determined via endpoint, this only increments the range
        to query.
        """
        if self.offset is None:
            self.offset = 0
        else:
            self.offset += self.batch_limit
        return {}

    def get_should_continue(self, response_json):
        """
        Override for upstream "return True". Dated runs will only ever make 1 query so
        they should not continue to loop.
        """
        return not bool(self.date)

    @staticmethod
    def _get_response_data(response_json) -> dict | list | None:
        """
        Intermediate method for pulling out results from a Phylopic API request.
        """
        if response_json and response_json.get("success") is True:
            return response_json.get("result")

    def get_batch_data(self, response_json):
        """
        Process the returned IDs.

        The Phylopic API returns only lists of IDs in the initial request. We must take
        this request and iterate through all the IDs to get the metadata for each one.
        """
        data = self._get_response_data(response_json)

        if not data:
            logger.warning("No content available!")
            return None

        return data

    @staticmethod
    def _image_url(_uuid: str) -> str:
        return f"{PhylopicDataIngester.host}/image/{_uuid}"

    @staticmethod
    def _get_image_info(
        result: dict, _uuid: str
    ) -> tuple[str | None, int | None, int | None]:
        img_url = None
        width = None
        height = None

        image_info = result.get("pngFiles")
        if image_info:
            images = list(
                filter(lambda x: (int(str(x.get("width", "0"))) >= 257), image_info)
            )
            if images:
                image = sorted(images, key=lambda x: x["width"], reverse=True)[0]
                img_url = image.get("url")
                if not img_url:
                    logging.warning(
                        "Image not detected in url: "
                        f"{PhylopicDataIngester._image_url(_uuid)}"
                    )
                else:
                    img_url = f"{PhylopicDataIngester.host}{img_url}"
                    width = image.get("width")
                    height = image.get("height")

        return img_url, width, height

    @staticmethod
    def _get_taxa_details(result: dict) -> tuple[list[str] | None, str]:
        taxa = result.get("taxa", [])
        # [0].get('canonicalName', {}).get('string')
        taxa_list = None
        title = ""
        if taxa:
            taxa = [
                _.get("canonicalName")
                for _ in taxa
                if _.get("canonicalName") is not None
            ]
            taxa_list = [_.get("string", "") for _ in taxa]

        if taxa_list:
            title = taxa_list[0]

        return taxa_list, title

    @staticmethod
    def _get_creator_details(result: dict) -> tuple[str | None, str | None, str | None]:
        credit_line = None
        pub_date = None
        creator = None
        submitter = result.get("submitter", {})
        first_name = submitter.get("firstName")
        last_name = submitter.get("lastName")
        if first_name and last_name:
            creator = f"{first_name} {last_name}".strip()

        if credit := result.get("credit"):
            credit_line = credit.strip()
            pub_date = result.get("submitted").strip()

        return creator, credit_line, pub_date

    @staticmethod
    def _get_meta_data(result: dict) -> dict | None:
        meta_data = {}
        _uuid = result.get("uid")
        license_url = result.get("licenseURL")

        img_url, width, height = PhylopicDataIngester._get_image_info(result, _uuid)

        if img_url is None:
            return None

        meta_data["taxa"], title = PhylopicDataIngester._get_taxa_details(result)

        foreign_url = PhylopicDataIngester._image_url(_uuid)

        (
            creator,
            meta_data["credit_line"],
            meta_data["pub_date"],
        ) = PhylopicDataIngester._get_creator_details(result)

        return {
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

    def get_record_data(self, data: dict) -> dict | list[dict] | None:
        _uuid = data.get("uid")
        if not _uuid:
            return
        logger.debug(f"Processing UUID: {_uuid}")
        params = {
            "options": " ".join(
                [
                    "credit",
                    "licenseURL",
                    "pngFiles",
                    "submitted",
                    "submitter",
                    "taxa",
                    "canonicalName",
                    "string",
                    "firstName",
                    "lastName",
                ]
            )
        }
        endpoint = f"{self.base_endpoint}/{_uuid}"
        response_json = self.get_response_json(params, endpoint)
        result = self._get_response_data(response_json)
        if not result:
            return None

        return self._get_meta_data(result)


def main(date_start: str = None, process_days: int = None):
    logger.info("Begin: Phylopic provider script")
    ingester = PhylopicDataIngester(date=date_start)
    ingester.default_process_days = process_days or ingester.default_process_days
    ingester.ingest_records()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PhyloPic API Job", add_help=True)
    parser.add_argument(
        "--date-start",
        default=None,
        help="Identify all images starting from a particular date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--process-days",
        default=None,
        help="Used in conjunction with --date-start, number of days to process starting"
        f"from --date-start, defaults to {PhylopicDataIngester.default_process_days} "
        "if not defined.",
    )

    args = parser.parse_args()

    main(args.date_start, args.process_days)

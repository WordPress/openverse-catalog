"""
A minimal template for a `ProviderDataIngester` subclass, which implements the
bare minimum of variables and methods.
"""
import logging

from common import constants
from common.license import get_license_info
from common.loader import provider_details as prov
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


logger = logging.getLogger(__name__)


class MyProviderDataIngester(ProviderDataIngester):
    providers = {
        "image": prov.TEMPLATE_IMAGE_PROVIDER,
    }
    endpoint = "https://my-api-endpoint"

    def get_next_query_params(self, prev_query_params: dict | None, **kwargs) -> dict:
        # On the first request, `prev_query_params` will be `None`. We can detect this
        # and return our default params
        if not prev_query_params:
            return {"limit": self.batch_limit, "cc": 1, "offset": 0}
        else:
            # Example case where the offset is incremented for each subsequent request.
            return {
                **prev_query_params,
                "offset": prev_query_params["offset"] + self.batch_limit,
            }

    def get_batch_data(self, response_json):
        # Takes the raw API response from calling `get` on the endpoint, and returns
        # the list of records to process.
        if response_json:
            return response_json.get("results")
        return None

    def get_media_type(self, record: dict):
        # For a given record json, return the media type it represents. May be
        # hard-coded if the provider only returns records of one type.
        return constants.IMAGE

    def get_record_data(self, data: dict) -> dict | list[dict] | None:
        # Parse out the necessary info from the record data into a dictionary.

        # If a required field is missing, return early to prevent unnecesary
        # processing.
        if (foreign_landing_url := data.get("url")) is None:
            return None
        if (image_url := data.get("image_url")) is None:
            return None

        # Use the common get_license_info method to get license information
        license_url = data.get("license")
        license_info = get_license_info(license_url)
        if license_info is None:
            return None

        # This example assumes the API guarantees all fields. Note that other
        # media types may have different fields available.
        return {
            "foreign_landing_url": foreign_landing_url,
            "image_url": image_url,
            "license_info": license_info,
            # Optional fields
            "foreign_identifier": data.get("foreign_id"),
            "thumbnail_url": data.get("thumbnail"),
            "filesize": data.get("filesize"),
            "filetype": data.get("filetype"),
            "creator": data.get("creator"),
            "creator_url": data.get("creator_url"),
            "title": data.get("title"),
            "meta_data": data.get("meta_data"),
            "raw_tags": data.get("tags"),
            "watermarked": data.get("watermarked"),
            "width": data.get("width"),
            "height": data.get("height"),
        }


def main():
    # Allows running ingestion from the CLI without Airflow running for debugging
    # purposes.
    logger.info("Begin: MyProvider data ingestion")
    ingester = MyProviderDataIngester()
    ingester.ingest_records()


if __name__ == "__main__":
    main()

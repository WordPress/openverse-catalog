"""
Content Provider:       Nappy

ETL Process:            Use the API to identify all CC0-licensed images.

Output:                 TSV file containing the image meta-data.

Notes:                  This api was written specially for Openverse.
                        There are no known limits or restrictions.

"""
import logging

from common import constants
from common.licenses import get_license_info
from common.loader import provider_details as prov
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


logger = logging.getLogger(__name__)


class NappyDataIngester(ProviderDataIngester):
    providers = {"image": prov.NAPPY_DEFAULT_PROVIDER}
    endpoint = "https://api.nappy.co/v1/openverse/images"
    headers = {"User-Agent": prov.UA_STRING, "Accept": "application/json"}

    def get_next_query_params(self, prev_query_params: dict | None, **kwargs) -> dict:
        # On the first request, `prev_query_params` will be `None`. We can detect this
        # and return our default params.
        if not prev_query_params:
            return {
                "page": 1,
            }
        else:
            return {
                **prev_query_params,
                "page": prev_query_params["page"] + 1,
            }

    def get_batch_data(self, response_json):
        # Takes the raw API response from calling `get` on the endpoint, and returns
        # the list of records to process.
        if response_json:
            return response_json.get("images")
        return None

    def get_should_continue(self, response_json):
        return bool(response_json.get("next_page"))

    def get_media_type(self, record: dict):
        return constants.IMAGE

    def get_record_data(self, data: dict) -> dict | list[dict] | None:
        # Parse out the necessary info from the record data into a dictionary.

        if (foreign_landing_url := data.get("foreign_landing_url")) is None:
            return None

        if (image_url := data.get("url")) is None:
            return None

        # Hardoded to CC0, the only license Nappy.co uses
        license_info = get_license_info(
            "https://creativecommons.org/publicdomain/zero/1.0/"
        )
        if license_info is None:
            return None

        def _convert_filesize(raw_filesize_string: str) -> int:
            """
            Convert sizes from strings to byte integers, ex. "187.8kB" to 188.
            """
            filetype_multipliers = {"kB": 1000, "MB": 1_000_000, "GB": 1_000_000_000}
            multiplier = filetype_multipliers[raw_filesize_string[-2:]]

            return int(round(float(data.get("filesize")[:-2]) * multiplier))

        # OPTIONAL FIELDS
        # Obtain as many optional fields as possible.
        foreign_identifier = data.get("foreign_identifier")
        thumbnail_url = data.get("url") + "?auto=format&w=600&q=75"
        filesize = _convert_filesize(data.get("filesize"))
        filetype = data.get("filetype")
        creator = data.get("creator")
        creator_url = data.get("creator_url")
        title = data.get("title")
        meta_data = data.get("meta_data")
        raw_tags = data.get("tags").split(",")
        width = data.get("width")
        height = data.get("height")

        return {
            "foreign_landing_url": foreign_landing_url,
            "image_url": image_url,
            "license_info": license_info,
            "foreign_identifier": foreign_identifier,
            "thumbnail_url": thumbnail_url,
            "filesize": filesize,
            "filetype": filetype,
            "creator": creator,
            "creator_url": creator_url,
            "title": title,
            "meta_data": meta_data,
            "raw_tags": raw_tags,
            "width": width,
            "height": height,
        }


def main():
    # Allows running ingestion from the CLI without Airflow running for debugging
    # purposes.
    logger.info("Begin: Nappy data ingestion")
    ingester = NappyDataIngester()
    ingester.ingest_records()


if __name__ == "__main__":
    main()

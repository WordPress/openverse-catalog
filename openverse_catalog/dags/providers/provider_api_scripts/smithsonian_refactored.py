import logging

from airflow.models import Variable
from common import constants
from common.license import get_license_info
from common.loader import provider_details as prov
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


logger = logging.getLogger(__name__)


class SmithsonianDataIngester(ProviderDataIngester):
    providers = {
        "image": prov.SMITHSONIAN_DEFAULT_PROVIDER,
    }
    endpoint = "https://api.si.edu/openaccess/api/v1.0/"
    delay = 0.5
    batch_limit = 1000
    headers = {}

    def get_next_query_params(self, prev_query_params: dict | None, **kwargs) -> dict:
        # On the first request, `prev_query_params` will be `None`. We can detect this
        # and return our default params.
        if not prev_query_params:
            return {
                "api_key": Variable.get("API_KEY_DATA_GOV"),
                "rows": self.batch_limit,
            }
        else:
            # TODO: Update any query params that change on subsequent requests.
            # Example case shows the offset being incremented by batch limit.
            return {
                **prev_query_params,
                "offset": prev_query_params["offset"] + self.batch_limit,
            }

    def get_batch_data(self, response_json):
        # Takes the raw API response from calling `get` on the endpoint, and returns
        # the list of records to process.
        # TODO: Update based on your API.
        if response_json:
            return response_json.get("results")
        return None

    def get_media_type(self, record: dict) -> str:
        return constants.IMAGE

    def get_record_data(self, data: dict) -> dict | list[dict] | None:
        # Parse out the necessary info from the record data into a dictionary.
        # TODO: Update based on your API.
        # TODO: Important! Refer to the most up-to-date documentation about the
        # available fields in `openverse_catalog/docs/data_models.md`

        # REQUIRED FIELDS:
        # - foreign_landing_url
        # - license_info
        # - image_url / audio_url
        #
        # If a required field is missing, return early to prevent unnecesary
        # processing.

        if (foreign_landing_url := data.get("url")) is None:
            return None

        # TODO: Note the url field name differs depending on field type. Append
        # `image_url` or `audio_url` depending on the type of record being processed.
        if (image_url := data.get("image_url")) is None:
            return None

        # Use the `get_license_info` utility to get license information from a URL.
        license_url = data.get("license")
        license_info = get_license_info(license_url)
        if license_info is None:
            return None

        # OPTIONAL FIELDS
        # Obtain as many optional fields as possible.
        foreign_identifier = data.get("foreign_id")
        thumbnail_url = data.get("thumbnail")
        filesize = data.get("filesize")
        filetype = data.get("filetype")
        creator = data.get("creator")
        creator_url = data.get("creator_url")
        title = data.get("title")
        meta_data = data.get("meta_data")
        raw_tags = data.get("tags")
        watermarked = data.get("watermarked")

        # MEDIA TYPE-SPECIFIC FIELDS
        # Each Media type may also have its own optional fields. See documentation.
        # TODO: Populate media type-specific fields.
        # If your provider supports more than one media type, you'll need to first
        # determine the media type of the record being processed.

        return {
            "foreign_landing_url": foreign_landing_url,
            "image_url": image_url,
            "license_info": license_info,
            # Optional fields
            "foreign_identifier": foreign_identifier,
            "thumbnail_url": thumbnail_url,
            "filesize": filesize,
            "filetype": filetype,
            "creator": creator,
            "creator_url": creator_url,
            "title": title,
            "meta_data": meta_data,
            "raw_tags": raw_tags,
            "watermarked": watermarked,
            # TODO: Remember to add any media-type specific fields here
        }


def main():
    logger.info("Begin: SmithsonianRefactored data ingestion")
    ingester = SmithsonianDataIngester()
    ingester.ingest_records()


if __name__ == "__main__":
    main()

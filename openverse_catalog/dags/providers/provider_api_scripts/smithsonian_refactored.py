import logging
from typing import Tuple

from airflow.exceptions import AirflowException
from airflow.models import Variable
from common import constants
from common.licenses import get_license_info
from common.loader import provider_details as prov
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester
from retry import retry


logger = logging.getLogger(__name__)


class SmithsonianDataIngester(ProviderDataIngester):
    providers = {
        "image": prov.SMITHSONIAN_DEFAULT_PROVIDER,
    }
    endpoint = "https://api.si.edu/openaccess/api/v1.0/"
    delay = 0.5
    batch_limit = 1000
    hash_prefix_length = 2

    def __init__(self, *args):
        super().__init__(*args)
        self.api_key = Variable.get("API_KEY_DATA_GOV")
        self.units_endpoint = f"{self.endpoint}terms/unit_code"

    def get_next_query_params(self, prev_query_params: dict | None, **kwargs) -> dict:
        # On the first request, `prev_query_params` will be `None`. We can detect this
        # and return our default params.
        query_string = "online_media_type:Images AND media_usage:CC0"
        if hash_prefix := kwargs.get("hash_prefix"):
            query_string += f" AND hash:{hash_prefix}*"
        if unit_code := kwargs.get("unit_code"):
            query_string += f" AND unit_code:{unit_code}"

        if not prev_query_params:
            return {
                "api_key": self.api_key,
                "q": query_string,
                "rows": self.batch_limit,
                "start": 0,
            }
        else:
            return {
                **prev_query_params,
                "q": query_string,
                "start": prev_query_params["start"] + self.batch_limit,
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

    @retry(ValueError, tries=3, delay=1, backoff=2)
    def _get_unit_codes_from_api(self) -> set:
        query_params = {"api_key": self.api_key, "q": "online_media_type:Images"}
        response_json = self.get_response_json(
            endpoint=self.units_endpoint, query_params=query_params
        )
        unit_codes_from_api = set(response_json.get("response", {}).get("terms", []))

        if len(unit_codes_from_api) == 0:
            raise ValueError("No unit codes received.")

        return unit_codes_from_api

    @staticmethod
    def _get_new_and_outdated_unit_codes(
        unit_codes_from_api: set, sub_providers: dict
    ) -> Tuple[set, set]:
        if not sub_providers:
            sub_providers = prov.SMITHSONIAN_SUB_PROVIDERS
        current_unit_codes = set().union(*sub_providers.values())

        new_unit_codes = unit_codes_from_api - current_unit_codes
        outdated_unit_codes = current_unit_codes - unit_codes_from_api

        return new_unit_codes, outdated_unit_codes

    def validate_unit_codes_from_api(self) -> None:
        """
        Validates the SMITHSONIAN_SUB_PROVIDERS dictionary, and raises an exception if
        human intervention is needed to add or remove unit codes.
        """
        unit_codes_from_api = self._get_unit_codes_from_api()
        new_unit_codes, outdated_unit_codes = self._get_new_and_outdated_unit_codes(
            unit_codes_from_api
        )

        if bool(new_unit_codes) or bool(outdated_unit_codes):
            message = (
                "\n*Updates needed to the SMITHSONIAN_SUB_PROVIDERS dictionary*:\n\n"
            )

            if bool(new_unit_codes):
                codes_string = "\n".join(f"  - `{code}`" for code in new_unit_codes)
                message += "New unit codes must be added:\n"
                message += codes_string
                message += "\n"

            if bool(outdated_unit_codes):
                codes_string = "\n".join(
                    f"  - `{code}`" for code in outdated_unit_codes
                )
                message += "Outdated unit codes must be deleted:\n"
                message += codes_string

            logger.info(message)
            raise AirflowException(message)

    def _get_hash_prefixes(self):
        max_prefix = "f" * self.hash_prefix_length
        format_string = f"0{self.hash_prefix_length}x"
        for h in range(int(max_prefix, 16) + 1):
            yield format(h, format_string)

    def ingest_records(self, **kwargs) -> None:
        for hash_prefix in self._get_hash_prefixes():
            super().ingest_records(hash_prefix=hash_prefix)


def main():
    logger.info("Begin: SmithsonianRefactored data ingestion")
    ingester = SmithsonianDataIngester()
    logger.info("Validating Smithsonian sub-providers...")
    ingester.validate_unit_codes_from_api()
    ingester.ingest_records()


if __name__ == "__main__":
    main()

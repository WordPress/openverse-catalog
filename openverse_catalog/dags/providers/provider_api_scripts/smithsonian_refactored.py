import logging
from typing import Tuple

from airflow.exceptions import AirflowException
from airflow.models import Variable
from common import constants
from common.licenses import get_license_info
from common.loader import provider_details as prov
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester
from retry import retry


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class SmithsonianDataIngester(ProviderDataIngester):
    providers = {
        "image": prov.SMITHSONIAN_DEFAULT_PROVIDER,
    }
    sub_providers = prov.SMITHSONIAN_SUB_PROVIDERS
    base_endpoint = "https://api.si.edu/openaccess/api/v1.0/"
    delay = 0.5
    batch_limit = 1000
    hash_prefix_length = 2
    description_types = {
        "description",
        "summary",
        "caption",
        "notes",
        "description (brief)",
        "description (spanish)",
        "description (brief spanish)",
        "gallery label",
        "exhibition label",
        "luce center label",
        "publication label",
        "new acquisition label",
    }
    tag_types = ("date", "object_type", "topic", "place")

    def __init__(self, *args):
        super().__init__(*args)
        self.api_key = Variable.get("API_KEY_DATA_GOV")
        self.units_endpoint = f"{self.base_endpoint}terms/unit_code"
        self.license_info = get_license_info(
            license_url="https://creativecommons.org/publicdomain/zero/1.0/"
        )

    @property
    def endpoint(self):
        return f"{self.base_endpoint}search"

    def get_media_type(self, record: dict) -> str:
        return constants.IMAGE

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

    def get_batch_data(self, response_json) -> list | None:
        # Takes the raw API response from calling `get` on the endpoint, and returns
        # the list of records to process.
        if response_json:
            rows = response_json.get("response", {}).get("rows")
            return self._check_type(rows, list)
        return None

    def get_record_data(self, data: dict) -> dict | list[dict] | None:
        # Parse out the necessary info from the record data into a dictionary.
        images = []
        if image_list := self._get_image_list(data):

            if (foreign_landing_url := self._get_foreign_landing_url(data)) is None:
                return None

            meta_data = self._extract_meta_data(data)
            partial_image_data = {
                "foreign_landing_url": foreign_landing_url,
                "title": data.get("title"),
                "license_info": self.license_info,
                "source": self._extract_source(meta_data),
                # "creator": self._get_creator(data),
                # creator_url = data.get("creator_url")
                # thumbnail_url = data.get("thumbnail")
                # filesize = data.get("filesize")
                # filetype = data.get("filetype")
                "meta_data": meta_data,
                "raw_tags": self._extract_tags(data),
                # watermarked = data.get("watermarked")
            }
            images += self._process_image_list(image_list, partial_image_data)
        return images

    @retry(ValueError, tries=3, delay=1, backoff=2)
    def _get_unit_codes_from_api(self) -> set:
        query_params = {"api_key": self.api_key, "q": "online_media_type:Images"}
        response_json = self.get_response_json(
            endpoint=self.units_endpoint, query_params=query_params
        )
        unit_codes_from_api = set(response_json.get("response", {}).get("terms", []))

        if len(unit_codes_from_api) == 0:
            raise ValueError("No unit codes received")

        logger.debug(f"\nUnit codes received:\n{unit_codes_from_api}\n")
        return unit_codes_from_api

    @staticmethod
    def _get_new_and_outdated_unit_codes(unit_codes_from_api: set) -> Tuple[set, set]:
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

    @staticmethod
    def _check_type(unknown_input, required_type):
        """
        This function ensures that the input is of the required type.

        Required Arguments:

        unknown_input:  This can be anything
        required_type:  built-in type name/constructor

        This function will check whether the unknown_input is of type
        required_type. If it is, it returns the unknown_input value
        unchanged. If not, will return the DEFAULT VALUE FOR THE TYPE
        required_type. So, if unknown_input is a float 3.0 and the
        required_type is int, this function will return 0.

        This function will work for required_type in:
        str, int, float, complex, list, tuple, dict, set, bool, and bytes.

        This function will create a relatively high-level log message
        whenever it has to initialize a default value for type
        required_type.
        """
        logger.debug(f"Ensuring {unknown_input} is of type {required_type}")
        if unknown_input is None:
            logger.debug(f"{unknown_input} is of type {type(unknown_input)}.")
            typed_input = required_type()
        elif type(unknown_input) != required_type:
            logger.info(
                f"{unknown_input} is of type {type(unknown_input)}"
                f" rather than {required_type}."
            )
            typed_input = required_type()
        else:
            typed_input = unknown_input
        return typed_input

    def _get_content_dict(self, row):
        return self._check_type(row.get("content"), dict)

    def _get_descriptive_non_repeating_dict(self, row):
        logger.debug("Getting descriptive_non_repeating_dict from row")
        desc = self._get_content_dict(row).get("descriptiveNonRepeating")
        return self._check_type(desc, dict)

    def _get_indexed_structured_dict(self, row):
        logger.debug("Getting indexed_structured_dict from row")
        content = self._get_content_dict(row).get("indexedStructured")
        return self._check_type(content, dict)

    def _get_image_list(self, row):
        dnr_dict = self._get_descriptive_non_repeating_dict(row)
        online_media = self._check_type(dnr_dict.get("online_media"), dict)
        return self._check_type(online_media.get("media"), list)

    @staticmethod
    def _process_image_list(image_list, partial_image_data: dict) -> list:
        images = []
        for image_data in image_list:
            usage = image_data.get("usage", {}).get("access")
            if image_data.get("type") != "Images" or usage != "CC0":
                continue

            if (image_url := image_data.get("content")) is None:
                continue

            if (foreign_identifier := image_data.get("idsId")) is None:
                continue

            images.append(
                {
                    **partial_image_data,
                    "image_url": image_url,
                    "foreign_identifier": foreign_identifier,
                }
            )
        return images

    def _get_foreign_landing_url(self, row):
        logger.debug("Getting foreign_landing_url from row")
        dnr_dict = self._get_descriptive_non_repeating_dict(row)
        foreign_landing_url = dnr_dict.get("record_link")
        if foreign_landing_url is None:
            foreign_landing_url = dnr_dict.get("guid")

        return foreign_landing_url

    def _get_freetext_dict(self, row):
        logger.debug("Getting freetext_dict from row")
        freetext = self._get_content_dict(row).get("freetext")
        return self._check_type(freetext, dict)

    def _extract_meta_data(self, row) -> dict:
        freetext = self._get_freetext_dict(row)
        descriptive_non_repeating = self._get_descriptive_non_repeating_dict(row)
        description, label_texts = "", ""
        notes = self._check_type(freetext.get("notes"), list)

        for note in notes:
            label = self._check_type(note.get("label", ""), str)
            if label.lower().strip() in self.description_types:
                description += " " + self._check_type(note.get("content", ""), str)
            elif label.lower().strip() == "label text":
                label_texts += " " + self._check_type(note.get("content", ""), str)

        meta_data = {
            "unit_code": descriptive_non_repeating.get("unit_code"),
            "data_source": descriptive_non_repeating.get("data_source"),
        }

        if description:
            meta_data.update(description=description.strip())
        if label_texts:
            meta_data.update(label_text=label_texts.strip())

        return {k: v for (k, v) in meta_data.items() if v is not None}

    def _extract_source(self, meta_data):
        unit_code = meta_data.get("unit_code").strip()
        source = next(
            (s for s in self.sub_providers if unit_code in self.sub_providers[s]), None
        )
        if source is None:
            raise Exception(f"An unknown unit code value {unit_code} encountered ")
        return source

    def _extract_tags(self, row):
        indexed_structured = self._get_indexed_structured_dict(row)
        tag_lists_generator = (
            self._check_type(indexed_structured.get(key), list)
            for key in self.tag_types
        )
        return [tag for tag_list in tag_lists_generator for tag in tag_list if tag]

    def ingest_records(self, **kwargs) -> None:
        for hash_prefix in self._get_hash_prefixes():
            super().ingest_records(hash_prefix=hash_prefix)


def main():
    ingester = SmithsonianDataIngester()
    logger.info("Validating Smithsonian sub-providers...")
    ingester.validate_unit_codes_from_api()
    ingester.ingest_records()


if __name__ == "__main__":
    main()

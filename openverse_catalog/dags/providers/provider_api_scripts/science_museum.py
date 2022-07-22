import logging
import re
from typing import Dict, Optional, Tuple

from common.licenses import get_license_info
from common.loader import provider_details as prov
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


logger = logging.getLogger(__name__)

LIMIT = 100

CC0_LICENSE = get_license_info(license_="cc0", license_version="1.0")

YEAR_RANGES = [
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


class ScienceMuseumDataIngester(ProviderDataIngester):
    providers = {"image": prov.SCIENCE_DEFAULT_PROVIDER}
    endpoint = "https://collection.sciencemuseumgroup.org.uk/search/"
    batch_limit = 1000
    delay = 5
    headers = {"Accept": "application/json"}
    RECORD_IDS = []  # class variable to keep track of records pulled
    # I set this to 10 so that it stops right after the first year_range is processed

    def ingest_records(self, **kwargs):
        for year_range in YEAR_RANGES:
            super().ingest_records(year_range=year_range)

    def get_next_query_params(self, old_query_params, **kwargs):
        from_, to_ = kwargs["year_range"]
        if not old_query_params:
            # Return default query params on the first request
            return {
                "has_image": 1,
                "image_license": "CC",
                "page[size]": LIMIT,
                "page[number]": 0,
                "date[from]": from_,
                "date[to]": to_,
            }
        else:
            # Increment `skip` by the batch limit.
            return {
                **old_query_params,
                "date[from]": from_,
                "date[to]": to_,
                "page[number]": old_query_params["page[number]"] + 1,
            }

    def get_media_type(self, record):
        # This provider only supports Images.
        return "image"

    def get_batch_data(self, response_json):
        if response_json:
            return response_json.get("data")
        return None

    def get_record_data(self, record):
        id_ = record.get("id")
        if id_ in ScienceMuseumDataIngester.RECORD_IDS:
            return None
        ScienceMuseumDataIngester.RECORD_IDS.append(id_)
        foreign_landing_url = record.get("links", {}).get("self")
        if foreign_landing_url is None:
            return None
        attributes = record.get("attributes")
        if attributes is None:
            return None
        title = attributes.get("summary_title")
        creator = self._get_creator_info(attributes)

        metadata = self._get_metadata(attributes)
        multimedia = attributes.get("multimedia")
        if multimedia is None:
            return None
        images = []
        for image_data in multimedia:
            foreign_id = image_data.get("admin", {}).get("uid")
            if foreign_id is None:
                continue
            processed = image_data.get("processed")
            (
                image_url,
                height,
                width,
                filetype,
            ) = ScienceMuseumDataIngester._get_image_info(processed)
            if image_url is None:
                continue

            license_pair = self._get_license(image_data)
            if license_pair is None:
                # some items do not return license anywhere, but in the UI
                # they look like CC
                continue
            license_, version = license_pair
            license_info = get_license_info(license_=license_, license_version=version)
            image = {
                "foreign_identifier": foreign_id,
                "foreign_landing_url": foreign_landing_url,
                "image_url": image_url,
                "height": height,
                "width": width,
                "filetype": filetype,
                "license_info": license_info,
                "creator": creator,
                "title": title,
                "meta_data": metadata,
            }
            images.append(image)
        return images

    @staticmethod
    def _get_creator_info(attributes):
        creator_info = None
        if (life_cycle := attributes.get("lifecycle")) is not None:
            creation = life_cycle.get("creation")
            if isinstance(creation, list):
                maker = creation[0].get("maker")
                if isinstance(maker, list):
                    creator_info = maker[0].get("summary_title")
        return creator_info

    @staticmethod
    def check_url(image_url: str | None) -> str | None:
        if not image_url:
            return None
        if image_url.startswith("http"):
            return image_url
        return f"https://coimages.sciencemuseumgroup.org.uk/images/{image_url}"

    @staticmethod
    def _get_dimensions(image_data: Dict) -> Tuple[int | None, int | None]:
        """
        Returns the height and width of the image from "image_data"."measurements"
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

    @staticmethod
    def _get_image_info(
        processed: Dict,
    ) -> Tuple[Optional[str], Optional[int], Optional[int], Optional[str]]:
        height, width, filetype = None, None, None
        image_data = processed.get("large")
        if image_data is None:
            image_data = processed.get("medium", {})

        image_url = ScienceMuseumDataIngester.check_url(image_data.get("location"))
        if image_url:
            filetype = image_data.get("format")
            height, width = ScienceMuseumDataIngester._get_dimensions(image_data)
        return image_url, height, width, filetype

    @staticmethod
    def _get_first_list_value(key: str, attributes: Dict) -> str | None:
        val = attributes.get(key)
        if isinstance(val, list):
            return val[0].get("value")
        return None

    @staticmethod
    def _get_metadata(attributes):
        metadata = {}
        for attr_key, metadata_key in [
            ("identifier", "accession number"),
            ("name", "name"),
            ("categories", "category"),
            ("description", "description"),
        ]:
            val = ScienceMuseumDataIngester._get_first_list_value(attr_key, attributes)
            if val is not None:
                metadata[metadata_key] = val

        creditline = attributes.get("legal")
        if isinstance(creditline, dict):
            line = creditline.get("credit_line")
            if line is not None:
                metadata["creditline"] = line

        return metadata

    @staticmethod
    def _get_license(image_data):
        rights = image_data.get("source", {}).get("legal", {}).get("rights")
        if isinstance(rights, list):
            license_name = rights[0].get("usage_terms")
            if not license_name:
                return None
            license_name = license_name.lower()
            license_name = re.sub("^cc[ -]", "", license_name)
            license_, version = license_name.split(" ")
            return license_, version
        return None


def main():
    logger.info("Begin: Science Museum data ingestion")
    ingester = ScienceMuseumDataIngester()
    ingester.ingest_records()


if __name__ == "__main__":
    main()

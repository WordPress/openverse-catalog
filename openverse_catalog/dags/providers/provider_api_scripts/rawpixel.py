"""
Content Provider:       Rawpixel

ETL Process:            Use the API to identify all CC-licensed images.

Output:                 TSV file containing the image meta-data.

Notes:                  Rawpixel has given us beta access to their API.
                        This API is undocumented, and we will need to contact Rawpixel
                        directly if we run into any issues.
                        The public API max results range is limited to 100,000 results,
                        although the API key we've been given can circumvent this limit.
                        https://www.rawpixel.com/api/v1/search?tags=$publicdomain&page=1&pagesize=100
"""
import logging
from urllib.parse import parse_qs, urlparse

from airflow.models import Variable
from common import constants
from common.licenses import get_license_info
from common.loader import provider_details as prov
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


logger = logging.getLogger(__name__)


class RawpixelDataIngester(ProviderDataIngester):
    providers = {constants.IMAGE: prov.RAWPIXEL_DEFAULT_PROVIDER}
    endpoint = "https://www.rawpixel.com/api/v1/search"
    batch_limit = 100

    def get_media_type(self, record: dict) -> str:
        return constants.IMAGE

    def get_next_query_params(self, prev_query_params: dict | None, **kwargs) -> dict:
        if not prev_query_params:
            return {
                "tags": "$publicdomain",
                "page": 1,
                "pagesize": self.batch_limit,
                # TODO: This is *not* correct, we actually need to construct this
                # signature cryptographically
                "s": Variable.get("API_KEY_RAWPIXEL"),  # s -> "signature"
            }
        else:
            return {**prev_query_params, "page": prev_query_params["page"] + 1}

    def get_batch_data(self, response_json):
        if response_json and (results := response_json.get("results")):
            return results
        return None

    @staticmethod
    def _get_image_properties(image, foreign_url):
        img_url = image.get("image_opengraph")
        if img_url:
            # extract the dimensions from the query params because
            # the dimensions in the metadata are at times
            # inconsistent with the rescaled images
            query_params = urlparse(img_url)
            width = parse_qs(query_params.query).get("w", [])[0]
            height = parse_qs(query_params.query).get("h", [])[0]
            return [img_url, width, height]
        else:
            logger.warning(f"Image not detected in URL: {foreign_url}")
            return [None, None, None, None]

    @staticmethod
    def _get_title_owner(image):
        title = image.get("image_title", "")
        owner = image.get("artist_names", "")
        owner = owner.replace("(Source)", "").strip()
        return [title, owner]

    @staticmethod
    def _get_meta_data(image):
        description = image.get("pinterest_description")
        meta_data = {}
        if description:
            meta_data["description"] = description
        return meta_data

    @staticmethod
    def _get_tags(image):
        keywords = image.get("keywords_raw")
        if keywords:
            keyword_list = keywords.split(",")
            keyword_list = [
                word.strip()
                for word in keyword_list
                if word.strip() not in ["cc0", "creative commons", "creative commons 0"]
            ]
            return keyword_list
        else:
            return []

    def get_record_data(self, data: dict) -> dict | list[dict] | None:
        # verify the license and extract the metadata
        license_ = "cc0"
        version = "1.0"

        if not (foreign_id := data.get("id")):
            return None

        if not (foreign_url := data.get("url")):
            return None

        img_url, width, height = self._get_image_properties(data, foreign_url)
        if not img_url:
            return None
        title, owner = self._get_title_owner(data)
        meta_data = self._get_meta_data(data)
        tags = self._get_tags(data)

        # TODO:How to get license_url, creator_url, source, watermarked?
        license_info = get_license_info(
            license_=license_,
            license_version=version,
        )
        return {
            "foreign_landing_url": foreign_url,
            "image_url": img_url,
            "license_info": license_info,
            "foreign_identifier": str(foreign_id),
            "width": str(width) if width else None,
            "height": str(height) if height else None,
            "title": title if title else None,
            "meta_data": meta_data,
            "raw_tags": tags,
            "creator": owner,
        }


def main():
    logger.info("Begin: RawPixel API ingestion")
    ingester = RawpixelDataIngester()
    ingester.ingest_records()


if __name__ == "__main__":
    main()

"""
Content Provider:       PhyloPic

ETL Process:            Use the API to identify all CC licensed images.

Output:                 TSV file containing the image,
                        their respective meta-data.

Notes:                  http://api-docs.phylopic.org/v2/
                        No rate limit specified.
"""

import logging

from common import constants
from common.licenses import get_license_info
from common.loader import provider_details as prov
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


logger = logging.getLogger(__name__)


class PhylopicDataIngester(ProviderDataIngester):
    delay = 5
    host = "https://www.phylopic.org"
    endpoint = "https://api.phylopic.org/images"
    providers = {constants.IMAGE: prov.PHYLOPIC_DEFAULT_PROVIDER}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.current_page = 0
        self.total_pages = 0
        self.build_param = 0

    def ingest_records(self):
        """Get initial params from the API and set the total pages."""
        resp = self.get_response_json(query_params={})
        if not resp:
            raise Exception("No response from Phylopic API.")
        self.build_param = resp.get("build")
        self.total_pages = resp.get("totalPages")
        logger.info(
            f"Total items to fetch: {resp.get('totalItems')}. "
            f"Total pages: {self.total_pages}"
        )

        super().ingest_records()

    def get_next_query_params(self, prev_query_params: dict | None, **kwargs) -> dict:
        if prev_query_params is not None:
            self.current_page += 1

        return {
            "build": self.build_param,
            "page": self.current_page,
            "embed_items": "true",
        }

    def get_should_continue(self, response_json):
        return self.current_page < self.total_pages

    def get_batch_data(self, response_json):
        return response_json.get("_embedded", {}).get("items", [])

    @staticmethod
    def _get_image_sizes(data: dict) -> tuple[int | None, int | None]:
        width, height = None, None
        sizes = data.get("sourceFile", {}).get("sizes")
        if sizes and "x" in sizes:
            width, height = sizes.split("x")
            # SVG sizes include decimal points so we get an approximation.
            width, height = int(float(width)), int(float(height))
        return width, height

    def get_record_data(self, data: dict) -> dict | list[dict] | None:
        """
        Get the data for a single image record.

        TODO: Adapt `url` and `creator_url` to avoid redirects.
        """
        uid = data.get("uuid")
        if not uid:
            return None

        data = data.get("_links", {})
        license_url = data.get("license", {}).get("href")
        img_url = data.get("sourceFile", {}).get("href")
        foreign_url = data.get("self", {}).get("href")
        if not license_url or not img_url or not foreign_url:
            return None

        foreign_url = self.host + foreign_url

        title = data.get("self", {}).get("title")
        creator = data.get("contributor", {}).get("title")
        creator_url = self.host + data.get("contributor", {}).get("href")
        width, height = self._get_image_sizes(data)

        return {
            "license_info": get_license_info(license_url=license_url),
            "foreign_identifier": uid,
            "foreign_landing_url": foreign_url,
            "image_url": img_url,
            "title": title,
            "creator": creator,
            "creator_url": creator_url,
            "width": width,
            "height": height,
            # TODO: Evaluate whether to include upstream thumbnails.
            # Sizes available: 192x192, 128x128, 64x64.
            # "thumbnail": thumbnail,
            # TODO: Evaluate whether to include nodes' titles as tags.
            # "tags": tags,
        }


def main():
    logger.info("Begin: Phylopic provider script")
    ingester = PhylopicDataIngester()
    ingester.ingest_records()


if __name__ == "__main__":
    main()

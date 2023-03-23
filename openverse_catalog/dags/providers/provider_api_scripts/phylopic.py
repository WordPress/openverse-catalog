"""
Content Provider:       PhyloPic

ETL Process:            Use the API to identify all CC licensed images.

Output:                 TSV file containing the image,
                        their respective meta-data.

Notes:                  http://api-docs.phylopic.org/v2/
                        No rate limit specified.
"""

import argparse
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
    def _get_image_info(
        result: dict, uid: str
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
                        f"{PhylopicDataIngester._image_url(uid)}"
                    )
                else:
                    img_url = f"{PhylopicDataIngester.host}{img_url}"
                    width = image.get("width")
                    height = image.get("height")

        return img_url, width, height

    @staticmethod
    def _image_url(uid: str) -> str:
        return f"{PhylopicDataIngester.host}/image/{uid}"

    # @staticmethod
    # def _get_taxa_details(result: dict) -> tuple[list[str] | None, str]:
    #     taxa = result.get("taxa", [])
    #     taxa_list = None
    #     title = ""
    #     if taxa:
    #         taxa = [
    #             _.get("canonicalName")
    #             for _ in taxa
    #             if _.get("canonicalName") is not None
    #         ]
    #         taxa_list = [_.get("string", "") for _ in taxa]
    #
    #     if taxa_list:
    #         title = taxa_list[0]
    #
    #     return taxa_list, title

    def get_record_data(self, data: dict) -> dict | list[dict] | None:
        uid = data.get("uuid")
        if not uid:
            return None

        data = data.get("_links", {})
        license_url = data.get("license", {}).get("href")
        img_url = data.get("sourceFile", {}).get("href")
        if not license_url or not img_url:
            return None

        # TODO: Adapt URL to avoid redirects
        foreign_url = self.host + data.get("self", {}).get("href")

        return {
            "foreign_identifier": uid,
            "foreign_landing_url": foreign_url,
            "image_url": img_url,
            "license_info": get_license_info(license_url=license_url),
            # "width": width,
            # "height": height,
            # "creator": creator,
            # "title": title,
            # "meta_data": meta_data,
        }


def main():
    logger.info("Begin: Phylopic provider script")
    ingester = PhylopicDataIngester()
    ingester.ingest_records()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PhyloPic API Job", add_help=True)
    parser.add_argument(
        "--date",
        default=None,
        help="Identify all images updated on a particular date (YYYY-MM-DD).",
    )

    args = parser.parse_args()

    main(args.date)

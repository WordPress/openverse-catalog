"""
Content Provider:       StockSnap

ETL Process:            Use the API to identify all CC-licensed images.

Output:                 TSV file containing the image, the respective meta-data.

Notes:                  https://stocksnap.io/faq
                        All images are licensed under CC0.
                        Seems like their API has been suspended, but this assumes
                        that there is a special agreement with Openverse.
"""
import json
import logging

from common.licenses import get_license_info
from common.loader import provider_details as prov
from providers.provider_api_scripts.cleveland_museum import ClevelandDataIngester
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

DELAY = 1  # in seconds
RETRIES = 3
HOST = "stocksnap.io"
ENDPOINT_BASE = f"https://{HOST}/api/load-photos/date/desc"
IMAGE_CDN = "https://cdn.stocksnap.io/img-thumbs/960w"
PROVIDER = prov.STOCKSNAP_DEFAULT_PROVIDER


class StockSnapDataIngester(ProviderDataIngester):
    providers = {"image": prov.STOCKSNAP_DEFAULT_PROVIDER}
    batch_limit = 1000
    delay = 1  # in seconds
    headers = {
        "Accept": "application/json",
    }

    def __init__(self):
        super(StockSnapDataIngester, self).__init__()
        self._page_counter = 1

    def get_next_query_params(self, old_query_params, **kwargs):
        # StockSnap uses /{page} at the end of the endpoint url instead of query params.
        # Page number is incremented in get_should_continue below.
        return None

    def get_media_type(self):
        return "image"

    @property
    def endpoint(self):
        return f"{ENDPOINT_BASE}/{self._page_counter}"

    def get_should_continue(self, response_json):
        if isinstance(response_json, list) and len(response_json) > 0:
            self._page_counter += 1
            return True
        else:
            return False

    def get_batch_data(self, response_json):
        """
        Take an API response and return the list of records.
        """
        # Based on the sample test file, using json.load on the file will return a list.
        if isinstance(response_json, list) and len(response_json) > 0:
            return response_json
        return None

    def get_record_data(self, data):
        """
        Parse out the necessary information (license info, urls, etc) from the record
        data (single image dict-like thing) into a dictionary.
        """
        try:
            foreign_id = data["img_id"]
        except (TypeError, KeyError, AttributeError):
            return None

        foreign_landing_url = f"https://{HOST}/photo/{foreign_id}"

        image_url, width, height = self._get_image_info(data)
        if image_url is None:
            logger.info("Found no image url.")
            logger.info(f"{json.dumps(data, indent=2)}")
            return None

        title = self._get_title(data)
        if title is None:
            logger.info("Found no image title.")
            logger.info(f"{json.dumps(data, indent=2)}")
            return None

        creator, creator_url = self._get_creator_data(data)
        metadata = self._get_metadata(data)
        tags = self._get_tags(data)
        filesize = self._get_filesize(image_url)
        lic = get_license_info("https://creativecommons.org/publicdomain/zero/1.0/")

        return {
            "title": title,
            "creator": creator,
            "creator_url": creator_url,
            "foreign_identifier": foreign_id,
            "foreign_landing_url": foreign_landing_url,
            "image_url": image_url,
            "filesize": filesize,
            "filetype": "jpg",
            "height": height,
            "width": width,
            "license_info": lic,
            "meta_data": metadata,
            "raw_tags": tags,
            "category": prov.DEFAULT_IMAGE_CATEGORY[PROVIDER],
        }

    @staticmethod
    def _get_image_info(item):
        width = ClevelandDataIngester._get_int_value(item, "img_width")
        height = ClevelandDataIngester._get_int_value(item, "img_height")
        img_id = item.get("img_id")
        image_url = f"{IMAGE_CDN}/{img_id}.jpg"
        return image_url, width, height

    @staticmethod
    def _get_creator_data(item):
        """
        Get the author's name and website preferring their custom link over the
        StockSnap profile. The latter is used if the first is not found.
        """
        creator_name = item.get("author_name")
        if creator_name is None:
            return None, None
        creator_url = item.get("author_website")
        if creator_url is None or creator_url in [
            "https://stocksnap.io/",
            "https://stocksnap.io/author/undefined/",
        ]:
            creator_url = item.get("author_profile")
        return creator_name, creator_url

    @staticmethod
    def _get_title(item):
        """
        Get the first two photo's tags/keywords to make the title and transform it
        to title case, as shown on its page.
        """
        tags = item.get("keywords", [])[:2]
        if len(tags) > 0:
            tags.append("Photo")
            img_title = " ".join(tags)
            return img_title.title()

    def _get_filesize(self, image_url):
        """
        Get the size of the image in bytes.
        """
        resp = self.delayed_requester.get(image_url)
        if resp:
            filesize = int(resp.headers.get("Content-Length", 0))
            return filesize if filesize != 0 else None

    @staticmethod
    def _get_metadata(item):
        """
        Include popularity statistics.
        """
        extras = ["downloads_raw", "page_views_raw", "favorites_raw"]
        metadata = {}
        for key in extras:
            value = item.get(key)
            if value is not None:
                metadata[key] = value
        return metadata

    @staticmethod
    def _get_tags(item):
        return item.get("keywords")


def main():
    logger.info("Begin: StockSnap data ingestion")
    ingester = StockSnapDataIngester()
    ingester.ingest_records()


if __name__ == "__main__":
    main()

"""
Content Provider:       WordPress Photo Directory

ETL Process:            Use the API to identify all openly licensed media.

Output:                 TSV file containing the media metadata.

Notes:                  https://wordpress.org/photos/wp-json/wp/v2
                        Provide photos, media, users and more related resources.
                        No rate limit specified.
"""
import logging
from pathlib import Path

import lxml.html as html
from common import constants
from common.licenses import get_license_info
from common.loader import provider_details as prov
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


logger = logging.getLogger(__name__)


class WordPressDataIngester(ProviderDataIngester):
    host = "wordpress.org"
    endpoint = f"https://{host}/photos/wp-json/wp/v2/photos"
    providers = {constants.IMAGE: prov.WORDPRESS_DEFAULT_PROVIDER}
    license_url = "https://creativecommons.org/publicdomain/zero/1.0/"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.license_info = get_license_info(license_url=self.license_url)

    def get_media_type(self, record: dict) -> str:
        return constants.IMAGE

    def get_next_query_params(self, prev_query_params: dict | None, **kwargs) -> dict:
        if not prev_query_params:
            return {
                "format": "json",
                "page": 1,
                "per_page": self.batch_limit,
                "_embed": "true",
            }
        else:
            return {**prev_query_params, "page": prev_query_params["page"] + 1}

    def get_batch_data(self, response_json):
        if isinstance(response_json, list) and len(response_json):
            return response_json
        return None

    def get_record_data(self, data):
        """
        Extract data for individual item.
        """
        if (foreign_identifier := data.get("slug")) is None:
            return None

        if (foreign_landing_url := data.get("link")) is None:
            return None

        try:
            media_details = (
                data.get("_embedded", {})
                .get("wp:featuredmedia", {})[0]
                .get("media_details", {})
            )
        except (KeyError, IndexError):
            return None

        image_url, height, width, filetype, filesize = self._get_file_info(
            media_details
        )
        if image_url is None:
            return None

        title = self._get_title(data)
        author, author_url = self._get_author_data(data)
        metadata, tags = self._get_metadata(data, media_details)

        return {
            "title": title,
            "creator": author,
            "creator_url": author_url,
            "foreign_identifier": foreign_identifier,
            "foreign_landing_url": foreign_landing_url,
            "image_url": image_url,
            "height": height,
            "width": width,
            "filetype": filetype,
            "filesize": filesize,
            "license_info": self.license_info,
            "meta_data": metadata,
            "raw_tags": tags,
        }

    def _get_file_info(self, media_details):
        preferred_sizes = ["2048x2048", "1536x1536", "medium_large", "large", "full"]
        for size in preferred_sizes:
            file_details = media_details.get("sizes", {}).get(size, {})
            image_url = file_details.get("source_url")
            if not image_url or image_url == "":
                continue

            height = file_details.get("height")
            width = file_details.get("width")
            filetype = None
            if filename := file_details.get("file"):
                filetype = Path(filename).suffix.replace(".", "")

            filesize = (
                media_details.get("filesize", 0)
                if size == "full"
                else file_details.get("filesize", 0)
            )
            if not filesize or int(filesize) == 0:
                filesize = self._get_filesize(image_url)

            return image_url, height, width, filetype, filesize
        return None, None, None, None, None

    def _get_filesize(self, image_url):
        resp = self.get_response_json(query_params={}, endpoint=image_url)
        if resp:
            filesize = int(resp.headers.get("Content-Length", 0))
            return filesize if filesize != 0 else None

    @staticmethod
    def _get_author_data(image):
        try:
            raw_author = image.get("_embedded", {}).get("author", [])[0]
        except IndexError:
            return None, None
        author = raw_author.get("name")
        if author is None or author == "":
            author = raw_author.get("slug")
        author_url = raw_author.get("url")
        if author_url == "":
            author_url = raw_author.get("link")
        return author, author_url

    @staticmethod
    def _get_title(image):
        if title := image.get("content", {}).get("rendered"):
            try:
                title = html.fromstring(title).text_content()
            except UnicodeDecodeError as e:
                logger.warning(f"Can't save the image's title ('{title}') due to {e}")
                return None
        return title

    @staticmethod
    def _get_metadata(media_data, media_details):
        raw_metadata = media_details.get("image_meta", {})
        metadata, tags = {}, []
        extras = [
            "aperture",
            "camera",
            "created_timestamp",
            "focal_length",
            "iso",
            "shutter_speed",
        ]
        for key in extras:
            value = raw_metadata.get(key)
            if value not in [None, ""]:
                metadata[key] = value

        raw_related_resources = media_data.get("_embedded", {}).get("wp:term", [])
        resource_mapping = {
            "photo_category": "categories",
            "photo_color": "colors",
            "photo_orientation": "orientation",
            "photo_tag": "tags",
        }
        for resource_arr in raw_related_resources:
            for resource in resource_arr:
                if (txy := resource.get("taxonomy")) in resource_mapping.keys():
                    resource_key = resource_mapping[txy]
                    resource_val = resource.get("name")
                    if txy == "photo_tag":
                        tags.append(resource_val)
                    elif txy == "photo_orientation":
                        metadata["orientation"] = resource_val
                    else:
                        metadata.setdefault(resource_key, [])
                        metadata[resource_key].append(resource_val)
        return metadata, tags


def main():
    ingester = WordPressDataIngester()
    ingester.ingest_records()


if __name__ == "__main__":
    main()

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
import base64
import hmac
import html
import logging
import re
from urllib.parse import urlencode

from airflow.models import Variable
from common import constants
from common.licenses import NO_LICENSE_FOUND, get_license_info
from common.loader import provider_details as prov
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


logger = logging.getLogger(__name__)


class RawpixelDataIngester(ProviderDataIngester):
    providers = {constants.IMAGE: prov.RAWPIXEL_DEFAULT_PROVIDER}
    api_path = "/api/v1/search"
    endpoint = f"https://www.rawpixel.com{api_path}"
    batch_limit = 100
    CC0 = "cc0"
    license_version = "1.0"
    # Regex pattern for partial suffix capture
    suffix_pattern_partial = re.compile(
        r"""
            # Any text ending in free or original
            (?:free|original)
            # Optionally "free public" or "original public"
            (?:\ public
                # Optionally "public domain"
                (?:\ domain
                    # Optionally "original public domain CC0 photo"
                    # or "free public domain CC0 image"
                    (?:\ CC0 (?:image|photo))?
                )?
            )?
            # Could end in punctuation, but the string must end with this sequence
            .?$
        """,
        flags=re.IGNORECASE | re.VERBOSE,
    )
    # Regex pattern for full suffix capture
    suffix_pattern_full = re.compile(
        r"""
            # Capture any of these complete endings
            (?:
                Free\ public\ domain\ CC0\ (?:image|photo) |
                Digitally\ enhanced\ by\ rawpixel
            )
            # Could end in punctuation, but the string must end with this sequence
            .?$
        """,
        flags=re.IGNORECASE | re.VERBOSE,
    )
    # Keywords which could be present in tags meaning they should be excluded
    tags_exclude_list = {
        "cc0",
        "creative commons",
        "public domain",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_key: str = Variable.get("API_KEY_RAWPIXEL")

    def get_media_type(self, record: dict) -> str:
        return constants.IMAGE

    def get_next_query_params(self, prev_query_params: dict | None, **kwargs) -> dict:
        if not prev_query_params:
            return {
                "tags": "$publicdomain",
                "page": 1,
                "pagesize": self.batch_limit,
            }
        else:
            return {**prev_query_params, "page": prev_query_params["page"] + 1}

    def _get_signature(self, query_params: dict) -> str:
        ordered_params = {k: v for k, v in sorted(query_params.items())}
        # URL encode the ordered parameters in a way that matches Node's
        # querystring.stringify as closely as possible
        # See: https://docs.python.org/3.10/library/urllib.parse.html#urllib.parse.urlencode  # noqa
        # and https://nodejs.org/api/querystring.html#querystringstringifyobj-sep-eq-options  # noqa
        query = urlencode(ordered_params, doseq=True)
        url = f"{self.api_path}?{query}"
        digest = hmac.digest(
            key=self.api_key.encode("utf-8"),
            msg=url.encode("utf-8"),
            digest="sha256",
        )
        b64 = base64.b64encode(digest)
        signature = b64.replace(b"+", b"-").replace(b"/", b"_").replace(b"=", b"")
        return signature.decode("utf-8")

    def get_response_json(
        self, query_params: dict, endpoint: str | None = None, **kwargs
    ):
        """
        Override of base get_response_json to inject API-key based HMAC signature.
        """
        signature = self._get_signature(query_params)
        query_params = {**query_params, "s": signature}
        return super().get_response_json(query_params, endpoint, **kwargs)

    def get_batch_data(self, response_json):
        if response_json and (results := response_json.get("results")):
            return results
        return None

    @staticmethod
    def _get_image_url(data: dict) -> str | None:
        """
        Access to the raw images is restricted in much the same way that the search API
        is restricted via API key + query param HMAC signature. Rawpixel provides two
        options for pseudo-direct URLs under the keys `google_teaser` and
        `pinterestImage`. These are scaled down to small and medium size images
        respectively, and the properties which define each are present in the query
        params.

        **Example**
        `google_teaser`: https://img.rawpixel.com/private/static/images/website/2022-05/upwk61670216-wikimedia-image-job572-1.jpg?w=800&dpr=1&fit=default&crop=default&q=65&vib=3&con=3&usm=15&bg=F4F4F3&ixlib=js-2.2.1&s=2d6a659c473e7a7ece331e3867a383fe
        `pinterestImage`: https://img.rawpixel.com/private/static/images/website/2022-05/upwk61670216-wikimedia-image-job572-1.jpg?w=1200&h=1200&dpr=1&fit=clip&crop=default&fm=jpg&q=75&vib=3&con=3&usm=15&cs=srgb&bg=F4F4F3&ixlib=js-2.2.1&s=12a67d2a2b59d6712886af7a73eb3045

        Due to the nature of HMAC signatures, we cannot strip the query params to access
        the full raw image without a signature, and we cannot use our API key to compute
        this signature (they likely have a private key they use for this). The
        `pinterestImage` is the highest quality direct URL we have access to, so we'll
        use that one.
        """  # noqa
        return data.get("pinterestImage")

    @staticmethod
    def _get_image_properties(data: dict) -> tuple[None, None] | tuple[int, int]:
        width = max(data.get("width", 0), data.get("display_image_width", 0))
        height = max(data.get("height", 0), data.get("display_image_height", 0))
        if (width, height) == (0, 0):
            return None, None
        return width, height

    @staticmethod
    def _clean_text(text: str) -> str:
        # First clear full patterns
        text = RawpixelDataIngester.suffix_pattern_full.sub("", text)
        # Then clear partial patterns
        text = RawpixelDataIngester.suffix_pattern_partial.sub("", text)
        # Unescape HTMl sequences
        text = html.unescape(text)
        # Clean whitespace
        text = text.strip()
        return text

    @staticmethod
    def _get_title(metadata: dict) -> str | None:
        """
        Titles come in the following form, so we clean them up a bit:
          Bull elk searches for food | Free Photo - rawpixel
          Desktop wallpaper summer beach landscape, | Free Photo - rawpixel
          Branch with a sunflower (1714&ndash;1760) | Free Photo Illustration - rawpixel
          Free public domain CC0 photo. | Free Photo - rawpixel
          Flower background. Free public domain | Free Photo - rawpixel
        """
        title: str = metadata.get("title")
        if not title:
            return None
        # Cut off the last bit about Rawpixel
        title = title.split("|", maxsplit=1)[0].strip()
        # Clean text
        title = RawpixelDataIngester._clean_text(title)
        # Remove trailing punctuation
        title = title.removesuffix(",").removesuffix(".")
        return title or None

    @staticmethod
    def _get_meta_data(data: dict, metadata: dict) -> dict:
        description = RawpixelDataIngester._clean_text(
            metadata.get("description_text") or ""
        )
        meta_data = {
            "description": description or None,
            "download_count": data.get("download_count"),
        }
        meta_data = {k: v for k, v in meta_data.items() if v is not None}
        return meta_data

    @staticmethod
    def _get_source(data: dict) -> str | None:
        source = data.get("artist_names", "")
        source = source.removesuffix("(Source)").strip()
        return source or None

    @staticmethod
    def _get_tags(metadata: dict) -> list[str]:
        keywords = metadata.get("popular_keywords")
        if keywords:
            return [
                keyword
                for keyword in keywords
                if not any(
                    exclude_tag in keyword
                    for exclude_tag in RawpixelDataIngester.tags_exclude_list
                )
            ]
        return []

    @staticmethod
    def _get_category(metadata: dict) -> str | None:
        keywords = set(metadata.get("popular_keywords", []))
        if "public domain art" in keywords:
            return prov.ImageCategory.DIGITIZED_ARTWORK.value
        if "image" in keywords or "photo" in keywords:
            return prov.ImageCategory.PHOTOGRAPH.value
        if "clipart" in keywords:
            return prov.ImageCategory.ILLUSTRATION.value
        return None

    def get_record_data(self, data: dict) -> dict | list[dict] | None:
        # verify the license and extract the metadata
        if not (foreign_id := data.get("id")):
            return None

        if not (foreign_url := data.get("url")):
            return None

        if not (metadata := data.get("metadata")):
            return None

        license_info = get_license_info(metadata["licenseUrl"])
        if license_info == NO_LICENSE_FOUND:
            return None

        if not (image_url := self._get_image_url(data)):
            return None

        width, height = self._get_image_properties(data)
        return {
            "foreign_landing_url": foreign_url,
            "image_url": image_url,
            "license_info": license_info,
            "foreign_identifier": foreign_id,
            "width": width,
            "height": height,
            "title": self._get_title(metadata),
            "meta_data": self._get_meta_data(data, metadata),
            "raw_tags": self._get_tags(metadata),
            "creator": self._get_source(data),
            "filetype": data.get("name_ext"),
            # Filesize not available via the API and wouldn't reflect final image size
            # anyway since we reference reduced-sized images
            "category": self._get_category(metadata),
        }


def main():
    logger.info("Begin: RawPixel API ingestion")
    ingester = RawpixelDataIngester()
    ingester.ingest_records()


if __name__ == "__main__":
    main()

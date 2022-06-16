import logging
from typing import Dict, List, Optional

from common.licenses import get_license_info
from common.loader import provider_details as prov
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


logger = logging.getLogger(__name__)

CC0_LICENSE = get_license_info(license_="cc0", license_version="1.0")


class ClevelandDataIngester(ProviderDataIngester):
    def __init__(
        self,
        providers={"image": prov.CLEVELAND_DEFAULT_PROVIDER},
        endpoint="http://openaccess-api.clevelandart.org/api/artworks/",
        delay=5,
        batch_limit=1000,
        retries=3,
        headers: Optional[Dict] = {},
    ):
        # Initialize the DataIngester with appropriate values
        super().__init__(providers, endpoint, delay, batch_limit, retries, headers)

    def get_next_query_params(self, old_query_params, **kwargs):
        if not old_query_params:
            # Return default query params on the first request
            return {"cc": "1", "has_image": "1", "limit": self.batch_limit, "skip": 0}
        else:
            # Increment `skip` by the batch limit.
            return {
                **old_query_params,
                "skip": old_query_params["skip"] + self.batch_limit,
            }

    def get_media_type(self, record):
        # This provider only supports Images.
        return "image"

    def get_record_data(self, record):
        license_ = record.get("share_license_status", "").lower()
        if license_ != "cc0":
            logger.error("Wrong license image")
            return None

        foreign_id = record.get("id")
        if foreign_id is None:
            return None

        image = self._get_image_type(record.get("images", {}))
        if image is None or image.get("url") is None:
            return None

        if record.get("creators"):
            creator_name = record.get("creators")[0].get("description", "")
        else:
            creator_name = ""
        
        return {
            "foreign_identifier": f"{foreign_id}",
            "foreign_landing_url": record.get("url"),
            "title": record.get("title", None),
            "creator": creator_name,
            "image_url": image["url"],
            "width": self._get_int_value(image, "width"),
            "height": self._get_int_value(image, "height"),
            "filesize": self._get_int_value(image, "filesize"),
            "license_info": CC0_LICENSE,
            "meta_data": self._get_metadata(record),
        }

    def _get_image_type(self, image_data):
        key, image_url = None, None
        if image_data.get("web"):
            key = "web"
            image_url = image_data.get("web").get("url", None)
        elif image_data.get("print"):
            key = "print"
            image_url = image_data.get("print").get("url", None)
        elif image_data.get("full"):
            key = "full"
            image_url = image_data.get("full").get("url", None)
        return image_url, key

    def _get_int_value(self, data: Dict, key: str) -> int | None:
        """
        Converts the value of the key `key` in `data` to an integer.
        Returns None if the value is not convertible to an integer, or
        if the value doesn't exist.
        """
        value = data.get(key)
        if bool(value):
            if isinstance(value, str) and value.isdigit():
                return int(value)
            elif isinstance(value, int):
                return value
        return None

    def _get_metadata(self, data):
        metadata = {
            "accession_number": data.get("accession_number", ""),
            "technique": data.get("technique", ""),
            "date": data.get("creation_date", ""),
            "credit_line": data.get("creditline", ""),
            "classification": data.get("type", ""),
            "tombstone": data.get("tombstone", ""),
            "culture": ",".join([i for i in data.get("culture", []) if i is not None]),
        }
        metadata = {k: v for k, v in metadata.items() if v is not None}
        return metadata

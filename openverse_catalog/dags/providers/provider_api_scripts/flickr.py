"""
Content Provider:       Flickr

ETL Process:            Use the API to identify all CC licensed images.

Output:                 TSV file containing the images and the
                        respective meta-data.

Notes:                  https://www.flickr.com/help/terms/api
                        Rate limit: 3600 requests per hour.
"""

import argparse
import logging
from datetime import datetime, timedelta, timezone

import lxml.html as html
from airflow.models import Variable
from common import constants
from common.licenses import get_license_info
from common.loader import provider_details as prov
from common.loader.provider_details import ImageCategory
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


logger = logging.getLogger(__name__)

LICENSE_INFO = {
    "1": ("by-nc-sa", "2.0"),
    "2": ("by-nc", "2.0"),
    "3": ("by-nc-nd", "2.0"),
    "4": ("by", "2.0"),
    "5": ("by-sa", "2.0"),
    "6": ("by-nd", "2.0"),
    "9": ("cc0", "1.0"),
    "10": ("pdm", "1.0"),
}


class FlickrDataIngester(ProviderDataIngester):
    provider_string = prov.FLICKR_DEFAULT_PROVIDER
    sub_providers = prov.FLICKR_SUB_PROVIDERS
    photo_url_base = prov.FLICKR_PHOTO_URL_BASE

    providers = {"image": provider_string}
    endpoint = "https://api.flickr.com/services/rest"
    batch_limit = 500
    retries = 5

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Because `ingest_records` is called multiple times, default query params
        # are built multiple times. Fetch keys and build parameters here so it
        # is done only once.
        self.api_key = Variable.get("API_KEY_FLICKR")
        self.license_param = ",".join(LICENSE_INFO.keys())

    @staticmethod
    def _derive_timestamp_pair_list(date):
        """ "
        Build a list of timestamp pairs that divide the given date into equal
        portions of the 24-hour period. Ingestion will be run separately for
        each of these time divisions. This is necessary because requesting data
        for the entire day at once may cause unexpected behavior from the API.

        We divide the day into 48 half-hour increments. #TODO:
        """
        seconds_in_a_day = 86400
        number_of_divisions = 48  # Divide the day into half-hour increments
        portion = int(seconds_in_a_day / number_of_divisions)
        utc_date = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        def _ts_string(d):
            return str(int(d.timestamp()))

        # Generate the start/end timestamps for each half-hour 'slice' of the day
        pair_list = [
            (
                _ts_string(utc_date + timedelta(seconds=i * portion)),
                _ts_string(utc_date + timedelta(seconds=(i + 1) * portion)),
            )
            for i in range(number_of_divisions)
        ]
        return pair_list

    def ingest_records(self, **kwargs):
        # Build a list of start/end timestamps demarcating portions of the day
        # for which to ingest data.
        timestamp_pairs = self._derive_timestamp_pair_list(self.date)

        for start_ts, end_ts in timestamp_pairs:
            logger.info(f"Ingesting data for start: {start_ts}, end: {end_ts}")
            super().ingest_records(start_timestamp=start_ts, end_timestamp=end_ts)

    def get_next_query_params(self, prev_query_params, **kwargs):
        start_timestamp = kwargs.get("start_timestamp")
        end_timestamp = kwargs.get("end_timestamp")

        if not prev_query_params:
            # Initial request, return default params
            return {
                "min_upload_date": start_timestamp,
                "max_upload_date": end_timestamp,
                "page": 0,
                "api_key": self.api_key,
                "license": self.license_param,
                "per_page": self.batch_limit,
                "method": "flickr.photos.search",
                "media": "photos",
                "safe_search": 1,  # Restrict to 'safe'
                "extras": (
                    "description,license,date_upload,date_taken,owner_name,tags,o_dims,"
                    "url_t,url_s,url_m,url_l,views,content_type"
                ),
                "format": "json",
                "nojsoncallback": 1,
            }
        else:
            # Increment the page number on subsequent requests
            return {**prev_query_params, "page": prev_query_params["page"] + 1}

    def get_media_type(self, record):
        # We only ingest images from Flickr
        return constants.IMAGE

    def get_batch_data(self, response_json):
        if response_json is None or response_json.get("stat") != "ok":
            return None
        return response_json.get("photos", {}).get("photo")

    def get_record_data(self, data):
        if (license_info := self._get_license_info(data)) is None:
            return None

        image_size = self._get_largest_image_size(data)
        if (image_url := data.get(f"url_{image_size}")) is None:
            return None

        if (foreign_id := data.get("id")) is None:
            return None

        if (owner := data.get("owner")) is None:
            # Owner is needed to construct the foreign_landing_url, which is
            # a required field
            return None

        creator_url = self._url_join(self.photo_url_base, owner.strip())
        foreign_landing_url = self._url_join(creator_url, foreign_id)

        # Optional fields
        height = data.get(f"height_{image_size}")
        width = data.get(f"width_{image_size}")
        title = data.get("title")
        creator = data.get("ownername")
        category = self._get_category(data)
        filesize, filetype = self._get_file_properties(image_url)
        meta_data = self._create_meta_data_dict(data)
        raw_tags = self._create_tags_list(data)
        # Flickr includes a collection of sub-providers which are available to a wide
        # audience. If this record belongs to a known sub-provider, we should indicate
        # that as the source. If not we fall back to the default provider.
        source = next(
            (s for s in self.sub_providers if owner in self.sub_providers[s]),
            self.provider_string,
        )

        return {
            "foreign_landing_url": foreign_landing_url,
            "image_url": image_url,
            "license_info": license_info,
            "foreign_identifier": foreign_id,
            "width": width,
            "height": height,
            "filesize": filesize,
            "filetype": filetype,
            "creator": creator,
            "creator_url": creator_url,
            "title": title,
            "meta_data": meta_data,
            "raw_tags": raw_tags,
            "source": source,
            "category": category,
        }

    def _url_join(self, *args):
        return "/".join([s.strip("/") for s in args])

    @staticmethod
    def _get_largest_image_size(image_data):
        """
        Returns the key for the largest image size available.
        """
        for size in ["l", "m", "s"]:
            if f"url_{size}" in image_data:
                return size

        logger.warning("No image detected!")
        return None

    @staticmethod
    def _get_license_info(image_data):
        license_id = str(image_data.get("license"))
        if license_id not in LICENSE_INFO:
            logger.warning(f"Unknown license ID: {license_id}")
            return None

        license_, license_version = LICENSE_INFO.get(license_id)
        return get_license_info(license_=license_, license_version=license_version)

    def _get_file_properties(self, image_url):
        """
        Get the size of the image in bytes and its filetype.
        """
        filesize, filetype = None, None
        if image_url:
            filetype = image_url.split(".")[-1]

            # TODO: Temporarily commenting out this code to avoid making an additional
            # request for every Flickr image to get filesize. We should test turning
            # this back on, or else find another way of getting the data.
            # Once re-enabled, we'll need to mock this method in the tests to prevent
            # making requests to Flickr while testing.
            #
            # resp = self.delayed_requester.get(image_url)
            # if resp:
            #     filesize = int(resp.headers.get("X-TTDB-L", 0))
        return (
            filesize if filesize != 0 else None,
            filetype if filetype != "" else None,
        )

    @staticmethod
    def _create_meta_data_dict(image_data, max_description_length=2000):
        meta_data = {
            "pub_date": image_data.get("dateupload"),
            "date_taken": image_data.get("datetaken"),
            "views": image_data.get("views"),
        }
        description = image_data.get("description", {}).get("_content", "")
        if description.strip():
            try:
                description_text = " ".join(
                    html.fromstring(description).xpath("//text()")
                ).strip()[:max_description_length]
                meta_data["description"] = description_text
            except (TypeError, ValueError, IndexError) as e:
                logger.warning(f"Could not parse description {description}!\n{e}")

        return {k: v for k, v in meta_data.items() if v is not None}

    @staticmethod
    def _create_tags_list(image_data, max_tag_string_length=2000):
        raw_tags = None
        # We limit the input tag string length, not the number of tags,
        # since tags could otherwise be arbitrarily long, resulting in
        # arbitrarily large data in the DB.
        raw_tag_string = image_data.get("tags", "").strip()[:max_tag_string_length]
        if raw_tag_string:
            # We sort for further consistency between runs, saving on
            # inserts into the DB later.
            raw_tags = sorted(list(set(raw_tag_string.split())))
        return raw_tags

    @staticmethod
    def _get_category(image_data):
        """
        Flickr has three types:
            0 for photos
            1 for screenshots
            3 for other
        Treating everything different from photos as unknown.
        """
        if "content_type" in image_data and image_data["content_type"] == "0":
            return ImageCategory.PHOTOGRAPH.value
        return None


def main(date):
    logger.info("Begin: Flickr data ingestion")
    ingester = FlickrDataIngester()
    ingester.ingest_records(date=date)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flickr API Job", add_help=True)
    parser.add_argument(
        "--date", help="Identify images uploaded on a date (format: YYYY-MM-DD)."
    )
    args = parser.parse_args()
    if args.date:
        date = args.date
    else:
        date_obj = datetime.now() - timedelta(days=2)
        date = datetime.strftime(date_obj, "%Y-%m-%d")

    main(date)

"""
Content Provider:       Wikimedia Commons

ETL Process:            Use the API to identify all CC-licensed images.

Output:                 TSV file containing the image, the respective
                        meta-data.

Notes:                  https://commons.wikimedia.org/wiki/API:Main_page
                        No rate limit specified.
"""

import argparse
import logging
from copy import deepcopy
from datetime import datetime, timedelta, timezone

import lxml.html as html
from common.constants import AUDIO, IMAGE
from common.licenses import get_license_info
from common.loader import provider_details as prov
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


logger = logging.getLogger(__name__)


class WikimediaCommonsDataIngester(ProviderDataIngester):
    providers = {
        "image": prov.WIKIMEDIA_DEFAULT_PROVIDER,
        "audio": prov.WIKIMEDIA_AUDIO_PROVIDER,
    }
    host = "commons.wikimedia.org"
    endpoint = f"https://{host}/w/api.php"
    headers = {"User-Agent": prov.UA_STRING}

    # The 10000 is a bit arbitrary, but needs to be larger than the mean
    # number of uses per file (globally) in the response_json, or we will
    # fail without a continuation token.  The largest example seen so far
    # had a little over 1000 uses
    mean_global_usage_limit = 10000
    # Total number of attempts to retrieve global usage pages for a given batch.
    # These can be very large, so we need to limit the number of attempts otherwise
    # the DAG will hit the timeout when it comes across these items.
    # See: https://github.com/WordPress/openverse-catalog/issues/725
    max_page_iteration_before_give_up = 10

    image_mediatypes = {"BITMAP", "DRAWING"}
    audio_mediatypes = {"AUDIO"}
    # Other types available in the API are OFFICE for pdfs and VIDEO

    # The batch_limit applies to the number of pages received by the API, rather
    # than the number of individual records. This means that for Wikimedia Commons
    # setting a global `INGESTION_LIMIT` will still limit the number of records, but
    # the exact limit may not be respected.
    batch_limit = 250

    class ReturnProps:
        """Different sets of properties to return from the API."""

        # All normal media info, plus popularity info by global usage
        all = "imageinfo|globalusage"
        # Just media info, used where there's too much global usage data to parse
        image_only = "imageinfo"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.start_timestamp, self.end_timestamp = self.derive_timestamp_pair(self.date)
        self.continue_token = {}
        self.popularity_cache: dict[str, int] = {}
        self.prop = self.ReturnProps.all

    def get_next_query_params(self, prev_query_params, **kwargs):
        return {
            "action": "query",
            "generator": "allimages",
            "gaisort": "timestamp",
            "gaidir": "newer",
            "gailimit": self.batch_limit,
            "prop": self.prop,
            "iiprop": "url|user|dimensions|extmetadata|mediatype|size|metadata",
            "gulimit": self.batch_limit,
            "gunamespace": 0,
            "format": "json",
            "gaistart": self.start_timestamp,
            "gaiend": self.end_timestamp,
            **self.continue_token,
        }

    def get_media_type(self, record):
        """Get the media_type of a parsed Record"""
        return record["media_type"]

    def get_response_json(self, query_params, **kwargs):
        """
        Get the response data from the API.

        Overrides the parent function to make multiple requests until
        we see "batchcomplete", rather than a single request to the
        endpoint. This ensures that global usage data used for calculating
        popularity is tabulated correctly.

        Occasionally, the ingester will come across a piece of media that is on many
        pages (and has lots of values for global usage). Due to the way Wikimedia's API
        handles batches, we have to iterate over each of the global usage responses in
        order to continue with the rest of the data. This frequently causes the DAG to
        timeout. To avoid this, we limit the number of iterations we make for parsing
        through a batch's global usage data. If we hit the limit, we re-issue the
        original query *without* requesting global usage data. This means that we will
        not have popularity data for these items the second time around. Especially
        since the problem with these images is that they're so popular, we want to
        preserve that information where possible! So we cache the popularity data from
        previous iterations and use it in subsequent ones if we come across the same
        item again.
        """
        batch_json = None
        gaicontinue = None
        iteration_count = 0

        for _ in range(self.mean_global_usage_limit):
            response_json = super().get_response_json(
                query_params,
                timeout=60,
            )

            if response_json is None:
                logger.warning("Response JSON is None")
                break
            else:
                # Update continue token for the next request
                self.continue_token = response_json.pop("continue", {})
                query_params.update(self.continue_token)
                logger.info(f"Continue token for next iteration: {self.continue_token}")

                current_gaicontinue = self.continue_token.get("gaicontinue", None)
                logger.debug(f"{current_gaicontinue=}")
                if (
                    current_gaicontinue is not None
                    and current_gaicontinue == gaicontinue
                ):
                    iteration_count += 1
                else:
                    gaicontinue = current_gaicontinue

                if iteration_count > self.max_page_iteration_before_give_up:
                    logger.warning(
                        f"Hit iteration count limit for '{gaicontinue}', "
                        "re-attempting with a bare token"
                    )
                    self.continue_token = {
                        "gaicontinue": gaicontinue,
                        "continue": "gaicontinue||",
                    }
                    self.prop = self.ReturnProps.image_only
                    break

                # Merge this response into the batch
                batch_json = self.merge_response_jsons(batch_json, response_json)

            if "batchcomplete" in response_json:
                logger.info("Found batchcomplete")
                # Reset the search props to include popularity data for the next iter
                self.prop = self.ReturnProps.all
                break
        return batch_json

    def get_should_continue(self, response_json):
        # Should not continue if continue_token is Falsy
        return self.continue_token

    def get_batch_data(self, response_json):
        image_pages = self.get_media_pages(response_json)
        if image_pages is not None:
            return image_pages.values()
        return None

    def get_media_pages(self, response_json):
        if response_json is not None:
            image_pages = response_json.get("query", {}).get("pages")
            if image_pages is not None:
                logger.info(f"Got {len(image_pages)} pages")
                return image_pages

        logger.warning(f"No pages in the image batch: {response_json}")
        return None

    def get_record_data(self, record):
        foreign_id = record.get("pageid")
        # logger.debug(f"Processing page ID: {foreign_id}")

        media_info = self.extract_media_info_dict(record)

        valid_media_type = self.extract_media_type(media_info)
        if not valid_media_type:
            # Do not process unsupported media types, like Video
            return None

        license_info = self.extract_license_info(media_info)
        if license_info.url is None:
            return None

        media_url = media_info.get("url")
        if media_url is None:
            return None

        creator, creator_url = self.extract_creator_info(media_info)
        title = self.extract_title(media_info)
        filesize = media_info.get("size", 0)  # in bytes
        filetype = self.extract_file_type(media_info)
        meta_data = self.create_meta_data_dict(record)

        record_data = {
            "media_url": media_url,
            "foreign_landing_url": media_info.get("descriptionshorturl"),
            "foreign_identifier": foreign_id,
            "license_info": license_info,
            "creator": creator,
            "creator_url": creator_url,
            "title": title,
            "filetype": filetype,
            "filesize": filesize,
            "meta_data": meta_data,
            "media_type": valid_media_type,
        }

        # Extend record_data with media-type specific fields
        funcs = {
            IMAGE: self.get_image_record_data,
            AUDIO: self.get_audio_record_data,
        }
        return funcs[valid_media_type](record_data, media_info)

    def get_image_record_data(self, record_data, media_info):
        """Extend record_data with image-specific fields."""
        record_data["image_url"] = record_data.pop("media_url")
        if record_data["filetype"] == "svg":
            record_data["category"] = "illustration"

        return {
            **record_data,
            "width": media_info.get("width"),
            "height": media_info.get("height"),
        }

    def get_audio_record_data(self, record_data, media_info):
        """Extend record_data with audio-specific fields."""
        record_data["audio_url"] = record_data.pop("media_url")

        duration = int(float(media_info.get("duration", 0)) * 1000)
        record_data["duration"] = duration
        record_data["category"] = self.extract_audio_category(record_data)

        file_metadata = self.parse_audio_file_meta_data(media_info)
        if sample_rate := self.get_value_by_names(
            file_metadata, ["audio_sample_rate", "sample_rate"]
        ):
            record_data["sample_rate"] = sample_rate
        if bit_rate := self.get_value_by_names(
            file_metadata, ["bitrate_nominal", "bitrate"]
        ):
            record_data["bit_rate"] = bit_rate if bit_rate <= 2147483647 else None
        if channels := self.get_value_by_names(
            file_metadata, ["audio_channels", "channels"]
        ):
            record_data["meta_data"]["channels"] = channels

        return record_data

    def parse_audio_file_meta_data(self, media_info):
        """Parse out audio file metadata."""
        metadata = media_info.get("metadata", [])

        streams = self.get_value_by_name(metadata, "streams")
        if not streams:
            audio = self.get_value_by_name(metadata, "audio")
            streams = self.get_value_by_name(audio, "streams")

        if streams:
            streams_data = streams[0].get("value", [])
            file_data = self.get_value_by_name(streams_data, "header")
            # Fall back to streams_data
            return file_data or streams_data

        return []

    @staticmethod
    def extract_media_info_dict(media_data):
        media_info_list = media_data.get("imageinfo")
        if media_info_list:
            media_info = media_info_list[0]
        else:
            media_info = {}
        return media_info

    @staticmethod
    def get_value_by_name(key_value_list: list, prop_name: str):
        """Get the first value for the given prop_name in a list of key value pairs."""
        if key_value_list is None:
            key_value_list = []

        prop_list = [
            key_value_pair
            for key_value_pair in key_value_list
            if key_value_pair["name"] == prop_name
        ]
        if prop_list:
            return prop_list[0].get("value")

    @staticmethod
    def get_value_by_names(key_value_list: list, prop_names: list):
        """Get the first available value for one of the `prop_names` property names."""
        for prop_name in prop_names:
            if val := WikimediaCommonsDataIngester.get_value_by_name(
                key_value_list, prop_name
            ):
                return val

    @staticmethod
    def extract_media_type(media_info):
        media_type = media_info.get("mediatype")
        image_mediatypes = WikimediaCommonsDataIngester.image_mediatypes
        audio_mediatypes = WikimediaCommonsDataIngester.audio_mediatypes

        if media_type in image_mediatypes:
            return IMAGE
        elif media_type in audio_mediatypes:
            return AUDIO

        logger.info(
            f"Incorrect mediatype: {media_type} not in "
            f"valid mediatypes ({image_mediatypes}, {audio_mediatypes})"
        )
        return None

    @staticmethod
    def extract_audio_category(parsed_data):
        """
        Determine the audio category.

        Sets category to "pronunciation" for any audio with pronunciation
        of a word or a phrase.
        """
        for category in parsed_data["meta_data"].get("categories", []):
            if "pronunciation" in category.lower():
                return "pronunciation"

    @staticmethod
    def extract_ext_value(media_info: dict, ext_key: str) -> str | None:
        return media_info.get("extmetadata", {}).get(ext_key, {}).get("value")

    @staticmethod
    def extract_title(media_info):
        # Titles often have 'File:filename.jpg' form
        # We remove the 'File:' and extension from title
        title = WikimediaCommonsDataIngester.extract_ext_value(media_info, "ObjectName")
        if title is None:
            title = media_info.get("title")
        if title.startswith("File:"):
            title = title.replace("File:", "", 1)
        last_dot_position = title.rfind(".")
        if last_dot_position > 0:
            possible_extension = title[last_dot_position:]
            if possible_extension.lower() in {".png", ".jpg", ".jpeg", ".ogg", ".wav"}:
                title = title[:last_dot_position]
        return title

    @staticmethod
    def extract_date_info(media_info):
        date_originally_created = WikimediaCommonsDataIngester.extract_ext_value(
            media_info, "DateTimeOriginal"
        )
        last_modified_at_source = WikimediaCommonsDataIngester.extract_ext_value(
            media_info, "DateTime"
        )
        return date_originally_created, last_modified_at_source

    @staticmethod
    def extract_creator_info(media_info):
        artist_string = WikimediaCommonsDataIngester.extract_ext_value(
            media_info, "Artist"
        )

        if not artist_string:
            return None, None

        artist_elem = html.fromstring(artist_string)
        # We take all text to replicate what is shown on Wikimedia Commons
        artist_text = "".join(artist_elem.xpath("//text()")).strip()
        url_list = list(artist_elem.iterlinks())
        artist_url = url_list[0][2] if url_list else None
        return artist_text, artist_url

    @staticmethod
    def extract_category_info(media_info):
        categories_string = (
            WikimediaCommonsDataIngester.extract_ext_value(media_info, "Categories")
            or ""
        )

        categories_list = categories_string.split("|")
        return categories_list

    @staticmethod
    def extract_file_type(media_info):
        filetype = media_info.get("url", "").split(".")[-1]
        return None if filetype == "" else filetype

    @staticmethod
    def extract_license_info(media_info):
        license_url = (
            WikimediaCommonsDataIngester.extract_ext_value(media_info, "LicenseUrl")
            or ""
        )
        # TODO Add public domain items
        # if license_url == "":
        #     license_name = extract_ext_value(media_info, "LicenseShortName") or ""
        #     if license_name.lower() in {"public_domain", "pdm-owner"}:
        #         pass

        license_info = get_license_info(license_url=license_url.strip())
        return license_info

    @staticmethod
    def extract_geo_data(media_data):
        geo_properties = {
            "latitude": "GPSLatitude",
            "longitude": "GPSLongitude",
            "map_datum": "GPSMapDatum",
        }
        geo_data = {}
        for (key, value) in geo_properties.items():
            key_value = media_data.get(value, {}).get("value")
            if key_value:
                geo_data[key] = key_value
        return geo_data

    def extract_global_usage(self, media_data) -> int:
        """
        Extract the global usage count from the media data.
        The global usage count serves as a proxy for popularity data, since it
        represents how many pages across all available wikis a media item is used on.
        This uses a cache to record previous values for a given foreign ID, because
        it is possible that we might encounter a media item several times based on the
        querying method used (see "props" above). Not all results will have the same
        (if any) usage data, so we want to record and cache the highest value. Most
        items have no global usage data, so we only cache items that have a value.
        """
        global_usage_count = len(media_data.get("globalusage", []))
        foreign_id = media_data["pageid"]
        cached_usage_count = self.popularity_cache.get(foreign_id, 0)
        max_usage_count = max(global_usage_count, cached_usage_count)
        # Only cache if it's greater than zero, otherwise it'll just take up space
        if max_usage_count > 0:
            self.popularity_cache[foreign_id] = max_usage_count
        return max_usage_count

    def create_meta_data_dict(self, media_data):
        media_info = self.extract_media_info_dict(media_data)
        date_originally_created, last_modified_at_source = self.extract_date_info(
            media_info
        )
        categories_list = self.extract_category_info(media_info)
        description = self.extract_ext_value(media_info, "ImageDescription")
        meta_data = {
            "global_usage_count": self.extract_global_usage(media_data),
            "date_originally_created": date_originally_created,
            "last_modified_at_source": last_modified_at_source,
            "categories": categories_list,
            **self.extract_geo_data(media_data),
        }
        if description:
            description_text = " ".join(
                html.fromstring(description).xpath("//text()")
            ).strip()
            meta_data["description"] = description_text
        return meta_data

    def merge_response_jsons(self, left_json, right_json):
        # Note that we will keep the continue value from the right json in
        # the merged output!  This is because we assume the right json is
        # the later one in the sequence of responses.
        if left_json is None:
            return right_json

        left_pages = self.get_media_pages(left_json)
        right_pages = self.get_media_pages(right_json)

        if (
            left_pages is None
            or right_pages is None
            or left_pages.keys() != right_pages.keys()
        ):
            logger.warning("Cannot merge responses with different pages!")
            merged_json = None
        else:
            merged_json = deepcopy(left_json)
            merged_json.update(right_json)
            merged_pages = self.get_media_pages(merged_json)
            merged_pages.update(
                {
                    k: self.merge_media_pages(left_pages[k], right_pages[k])
                    for k in left_pages
                }
            )

        return merged_json

    def merge_media_pages(self, left_page, right_page):
        merged_page = deepcopy(left_page)
        merged_globalusage = left_page.get("globalusage", []) + right_page.get(
            "globalusage", []
        )
        merged_page.update(right_page)
        merged_page["globalusage"] = merged_globalusage

        return merged_page

    def derive_timestamp_pair(self, date):
        date_obj = datetime.strptime(date, "%Y-%m-%d")
        utc_date = date_obj.replace(tzinfo=timezone.utc)
        start_timestamp = str(int(utc_date.timestamp()))
        end_timestamp = str(int((utc_date + timedelta(days=1)).timestamp()))
        logger.info(
            f"Start timestamp: {start_timestamp}, end timestamp: {end_timestamp}"
        )
        return start_timestamp, end_timestamp


def main(date):
    logger.info(f"Begin: Wikimedia Commons data ingestion for {date}")
    ingester = WikimediaCommonsDataIngester({"date": date})
    ingester.ingest_records()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Wikimedia Commons API Job",
        add_help=True,
    )
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

"""
Content Provider:       Freesound

ETL Process:            Use the API to identify all CC-licensed images.

Output:                 TSV file containing the image, the respective
                        meta-data.

Notes:                  https://freesound.org/apiv2/search/text'
                        No rate limit specified.
"""
import copy
import functools
import logging
from datetime import datetime

import requests
from airflow.models import Variable
from common import constants
from common.licenses.licenses import get_license_info
from common.loader import provider_details as prov
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester
from requests.exceptions import ConnectionError, SSLError
from retry import retry


logger = logging.getLogger(__name__)


class FreesoundDataIngester(ProviderDataIngester):
    batch_limit = 150
    RETRIES = 3
    host = "freesound.org"
    endpoint = f"https://{host}/apiv2/search/text"
    providers = {"audio": prov.FREESOUND_DEFAULT_PROVIDER}
    flaky_exceptions = (SSLError, ConnectionError)
    preview_bitrates = {
        "preview-hq-mp3": 128000,
        "preview-lq-mp3": 64000,
        "preview-hq-ogg": 192000,
        "preview-lq-ogg": 80000,
    }

    HEADERS = {
        "Accept": "application/json",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api_key = Variable.get("API_KEY_FREESOUND")
        self.headers = {"api_key": self.api_key}

    def get_media_type(self, record: dict) -> str:
        return constants.AUDIO

    def ingest_records(self, **kwargs):
        for license_name in [
            "Attribution",
            "Attribution Noncommercial",
            "Creative Commons 0",
        ]:
            logger.info(f"Obtaining audio records under license '{license_name}'")
            super().ingest_records(license_name=license_name, **kwargs)

    def get_next_query_params(self, prev_query_params: dict | None, **kwargs) -> dict:
        license_name = kwargs.get("license_name")
        if not prev_query_params:
            return {
                "format": "json",
                "token": self.api_key,
                "query": "",
                "page_size": self.batch_limit,
                "fields": "id,url,name,tags,description,created,license,type,download,"
                "filesize,bitrate,bitdepth,duration,samplerate,pack,username,"
                "num_downloads,avg_rating,num_ratings,geotag,previews",
                "license_name": license_name,
                "page": 1,
            }
        else:
            return {**prev_query_params, "page": prev_query_params["page"] + 1}

    def get_batch_data(self, response_json):
        if response_json:
            return response_json.get("results")
        return None


    @staticmethod
    def _get_creator_data(item):
        if creator := item.get("username"):
            creator = creator.strip()
            creator_url = f"https://freesound.org/people/{creator}/"
        else:
            creator_url = None
        return creator, creator_url


    @staticmethod
    def _get_metadata(item):
        metadata = {}
        fields = [
            "description",
            "num_downloads",
            "avg_rating",
            "num_ratings",
            "geotag",
            "download",
        ]
        for field in fields:
            if field_value := item.get(field):
                metadata[field] = field_value
        return metadata


    @staticmethod
    def _get_license(item):
        item_license = get_license_info(license_url=item.get("license"))

        if item_license.license is None:
            return None
        return item_license


def main(date="all"):
    """This script pulls the data for a given date from the Freesound,
    and writes it into a .TSV file to be eventually read
    into our DB.

    Required Arguments:

    date:  Date String in the form YYYY-MM-DD.  This is the date for
           which running the script will pull data.
    """

    logger.info("Begin: Freesound script")
    licenses = ["Attribution", "Attribution Noncommercial", "Creative Commons 0"]
    for license_name in licenses:
        audio_count = _get_items(license_name, date)
        logger.info(f"Audios for {license_name} pulled: {audio_count}")
    logger.info("Terminated!")


def _get_query_params(
    license_name="",
    page_number=1,
    default_query_params=None,
):
    default_query_params = default_query_params or DEFAULT_QUERY_PARAMS
    return {
        **default_query_params,
        "page": str(page_number),
        "license": license_name,
    }


def _get_items(license_name, date):
    item_count = 0
    page_number = 1
    should_continue = True
    default_query_params = copy.deepcopy(DEFAULT_QUERY_PARAMS)
    try:
        start_date = datetime.strftime(
            datetime.fromisoformat(date), "%Y-%m-%dT%H:%M:%SZ"
        )
    except ValueError:
        start_date = "*"
    default_query_params["filter"] = f"created:[{start_date} TO NOW]"
    while should_continue:
        query_param = _get_query_params(
            default_query_params=default_query_params,
            license_name=license_name,
            page_number=page_number,
        )
        batch_data = _get_batch_json(query_param=query_param)
        if batch_data:
            item_count = _process_item_batch(batch_data)
            page_number += 1
        else:
            should_continue = False
    return item_count


def _get_batch_json(endpoint=ENDPOINT, headers=None, retries=RETRIES, query_param=None):
    if headers is None:
        headers = HEADERS
    response_json = delayed_requester.get_response_json(
        endpoint, retries, query_param, headers=headers
    )
    if response_json is None:
        return None
    else:
        results = response_json.get("results")
        return results


def _process_item_batch(items_batch):
    for item in items_batch:
        # Freesound sometimes returns results that are just "None"
        if item is None:
            continue
        item_meta_data = _extract_audio_data(item)
        if item_meta_data is None:
            continue
        audio_store.add_item(**item_meta_data)
    return audio_store.total_items


def _extract_audio_data(media_data):
    """Extracts meta data about the audio file
    Freesound does not have audio thumbnails"""
    foreign_landing_url = media_data.get("url")
    if not foreign_landing_url:
        return None

    foreign_identifier = media_data.get("id")
    if not foreign_identifier:
        return None

    item_license = _get_license(media_data)
    if item_license is None:
        return None

    # We use the mp3-hq preview url as `audio_url` as the main url
    # for playing on the frontend,
    # and the actual uploaded file as an alt_file that is available
    # for download (and requires a user to be authenticated to download)
    try:
        main_audio, alt_files = _get_audio_files(media_data)
    except FLAKY_EXCEPTIONS:
        logger.warning(f"Unable to get file size for {foreign_landing_url}, skipping")
        return None
    if main_audio is None:
        return None

    creator, creator_url = _get_creator_data(media_data)
    duration = int(media_data.get("duration") * 1000)
    set_foreign_id, audio_set, set_url = _get_audio_set_info(media_data)
    return {
        "title": media_data.get("name"),
        "creator": creator,
        "creator_url": creator_url,
        "foreign_identifier": foreign_identifier,
        "foreign_landing_url": foreign_landing_url,
        "duration": duration,
        "license_info": item_license,
        "meta_data": _get_metadata(media_data),
        "raw_tags": media_data.get("tags"),
        "set_foreign_id": set_foreign_id,
        "audio_set": audio_set,
        "set_url": set_url,
        "alt_files": alt_files,
        # audio_url, filetype, bit_rate
        **main_audio,
    }


def _get_audio_set_info(media_data):
    # set id, set name, set url
    set_url = media_data.get("pack")
    if set_url is not None:
        set_id, set_name = _get_set_info(set_url)
        return set_id, set_name, set_url
    else:
        return None, None, None


@functools.lru_cache(maxsize=1024)
def _get_set_info(set_url):
    response_json = delayed_requester.get_response_json(
        set_url,
        3,
        query_params={"token": API_KEY},
    )
    set_id = response_json.get("id")
    set_name = response_json.get("name")
    return set_id, set_name


def _get_preview_filedata(preview_type, preview_url):
    return {
        "url": preview_url,
        "filetype": preview_type.split("-")[-1],
        "bit_rate": preview_bitrates[preview_type],
    }


@retry(FLAKY_EXCEPTIONS, tries=3, delay=1, backoff=2)
def _get_audio_file_size(url):
    """
    Get the content length of a provided URL.
    Freesound can be finicky, so we want to retry it a few times on these conditions:
      * SSLError - 'EOF occurred in violation of protocol (_ssl.c:1129)'
      * ConnectionError - '[Errno 113] No route to host'

    Both of these seem transient and may be the result of some odd behavior on the
    Freesound API end. We have an API key that's supposed to be maxed out, so I can't
    imagine it's throttling (aetherunbound).

    TODO(obulat): move filesize detection to the polite crawler
    """
    return int(requests.head(url).headers["content-length"])


def _get_audio_files(media_data):
    # This is the original file, needs auth for downloading.
    # bit_rate in kilobytes, converted to bytes
    alt_files = [
        {
            "url": media_data.get("download"),
            "bit_rate": int(media_data.get("bitrate")) * 1000,
            "sample_rate": int(media_data.get("samplerate")),
            "filetype": media_data.get("type"),
            "filesize": media_data.get("filesize"),
        }
    ]
    previews = media_data.get("previews")
    # If there are no previews, then we will not be able to play the file
    if not previews:
        return None
    main_file = _get_preview_filedata("preview-hq-mp3", previews["preview-hq-mp3"])
    main_file["audio_url"] = main_file.pop("url")
    main_file["filesize"] = _get_audio_file_size(main_file["audio_url"])
    return main_file, alt_files


if __name__ == "__main__":
    main()

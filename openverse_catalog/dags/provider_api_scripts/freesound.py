"""
Content Provider:       Freesound

ETL Process:            Use the API to identify all CC-licensed images.

Output:                 TSV file containing the image, the respective
                        meta-data.

Notes:                  https://freesound.org/apiv2/search/text'
                        No rate limit specified.
"""
import functools
import os
import logging
from urllib.parse import urlparse

from common.licenses.licenses import get_license_info
from common.requester import DelayedRequester
from storage.audio import AudioStore
from util.loader import provider_details as prov

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

LIMIT = 150
DELAY = 1  # in seconds
RETRIES = 3
HOST = "freesound.org"
ENDPOINT = f"https://{HOST}/apiv2/search/text"
PROVIDER = prov.FREESOUND_DEFAULT_PROVIDER
# Freesound only has 'sounds'
FREESOUND_CATEGORY = "sound"
API_KEY = os.getenv("FREESOUND_API_KEY", "not_set")

HEADERS = {
    "Accept": "application/json",
}
DEFAULT_QUERY_PARAMS = {
    "format": "json",
    "token": API_KEY,
    "query": "",
    "page_size": LIMIT,
    "fields": "id,url,name,tags,description,created,license,type,download,"
    "filesize,bitrate,bitdepth,duration,samplerate,pack,username,"
    "num_downloads,avg_rating,num_ratings,geotag,previews",
}

delayed_requester = DelayedRequester(DELAY)
audio_store = AudioStore(provider=PROVIDER)


def main():
    """This script pulls the data for a given date from the Freesound,
    and writes it into a .TSV file to be eventually read
    into our DB.
    """

    logger.info("Begin: Freesound script")
    licenses = ["Attribution", "Attribution Noncommercial", "Creative Commons 0"]
    for license_name in licenses:
        audio_count = _get_items(license_name)
        logger.info(f"Audios for {license_name} pulled: {audio_count}")
    logger.info("Terminated!")


def _get_query_params(
    license_name="",
    page_number=1,
    default_query_params=None,
):
    if default_query_params is None:
        default_query_params = DEFAULT_QUERY_PARAMS
    query_param = default_query_params.copy()
    query_param["page"] = str(page_number)
    query_param["license"] = license_name
    return query_param


def _get_items(license_name):
    item_count = 0
    page_number = 1
    should_continue = True
    while should_continue:
        query_param = _get_query_params(
            license_name=license_name, page_number=page_number
        )
        batch_data = _get_batch_json(query_param=query_param)
        if isinstance(batch_data, list) and len(batch_data) > 0:
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
        item_meta_data = _extract_audio_data(item)
        if item_meta_data is None:
            continue
        audio_store.add_item(**item_meta_data)
    return audio_store.total_items


def _extract_audio_data(media_data):
    """Extracts meta data about the audio file
    Freesound does not have audio thumbnails"""
    try:
        foreign_landing_url = media_data["url"]
    except (TypeError, KeyError, KeyError):
        return None
    foreign_identifier = _get_foreign_identifier(media_data)
    if foreign_identifier is None:
        return None
    audio_url, duration = _get_audio_info(media_data)
    if audio_url is None:
        return None
    item_license = _get_license(media_data)
    if item_license is None:
        return None
    creator, creator_url = _get_creator_data(media_data)
    file_properties = _get_file_properties(media_data)
    audio_set, set_url = _get_audio_set(media_data)

    return {
        "title": _get_title(media_data),
        "creator": creator,
        "creator_url": creator_url,
        "foreign_identifier": foreign_identifier,
        "foreign_landing_url": foreign_landing_url,
        "audio_url": audio_url,
        "duration": duration,
        "license_info": item_license,
        "meta_data": _get_metadata(media_data),
        "raw_tags": media_data.get("tags"),
        "audio_set": audio_set,
        "set_url": set_url,
        "alt_files": _get_alt_files(media_data),
        "category": FREESOUND_CATEGORY,
        **file_properties,
    }


def _get_audio_set(media_data):
    # set name, set thumbnail, position of audio in set, set url
    set_name = None
    set_url = media_data.get("pack")
    if set_url is not None:
        set_name = _get_set_name(set_url)
    return set_name, set_url


@functools.lru_cache(maxsize=1024)
def _get_set_name(set_url):
    response_json = delayed_requester.get_response_json(
        set_url,
        3,
        query_params={"token": API_KEY},
    )
    name = response_json.get("name")
    return name


def _get_alt_files(media_data):
    alt_files = [
        {
            "url": media_data.get("download"),
            "format": media_data.get("type"),
            "filesize": media_data.get("filesize"),
            "bit_rate": media_data.get("bitrate"),
            "sample_rate": media_data.get("samplerate"),
        }
    ]
    previews = media_data.get("previews")
    if previews is not None:
        for preview_type in previews:
            preview_url = previews[preview_type]
            alt_files.append(
                {"url": preview_url, "format": preview_type.split("-")[-1]}
            )
    return alt_files


def _get_file_properties(media_data):
    props = {
        "bit_rate": int(media_data.get("bitrate")),
        "sample_rate": int(media_data.get("samplerate")),
    }
    return props


def _get_foreign_identifier(media_data):
    try:
        return media_data["id"]
    except (TypeError, IndexError, KeyError):
        return None


def _get_audio_info(media_data):
    duration = int(media_data.get("duration") * 1000)
    # TODO: Decide whether to use
    # 1. foreign landing url - not playable
    # 2. download url - needs OAuth2
    # 3. mp3 preview - is probably lower quality. Is full file?
    # This URL is foreign_landing_url
    audio_url = media_data.get("url")
    return audio_url, duration


def _get_creator_data(item):
    creator = item.get("username", "").strip()
    if creator:
        creator_url = f"https://freesound.org/people/{creator}/"
    else:
        creator, creator_url = None, None
    return creator, creator_url


def _get_title(item):
    title = item.get("name")
    return title


def _get_metadata(item):
    # previews is a dictionary with URIs for mp3 and ogg
    # versions of the file (preview-hq-mp3 ~128kbps, preview-lq-mp3 ~64kbps
    # preview-hq-ogg ~192kbps, preview-lq-ogg ~80kbps).
    metadata = {}
    fields = [
        "description",
        "num_downloads",
        "avg_rating",
        "num_ratings",
        "geotag",
        "download",
        "previews",
    ]
    for field in fields:
        field_value = item.get(field)
        if field_value:
            metadata[field] = field_value
    return metadata


def _get_license(item):
    item_license_url = item.get("license")
    item_license = get_license_info(license_url=item_license_url)

    if item_license.license is None:
        return None
    return item_license


def _cleanse_url(url_string):
    """
    Check to make sure that a url is valid, and prepend a protocol if needed
    """

    parse_result = urlparse(url_string)

    if parse_result.netloc == HOST:
        parse_result = urlparse(url_string, scheme="https")
    elif not parse_result.scheme:
        parse_result = urlparse(url_string, scheme="http")

    if parse_result.netloc or parse_result.path:
        return parse_result.geturl()


if __name__ == "__main__":
    main()

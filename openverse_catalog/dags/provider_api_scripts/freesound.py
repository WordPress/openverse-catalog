"""
Content Provider:       Freesound

ETL Process:            Use the API to identify all CC-licensed images.

Output:                 TSV file containing the image, the respective
                        meta-data.

Notes:                  https://freesound.org/apiv2/search/text'
                        No rate limit specified.
"""
import functools
import logging
import os

from common.licenses.licenses import get_license_info
from common.requester import DelayedRequester
from storage.audio import AudioStore
from util.loader import provider_details as prov


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
    default_query_params = default_query_params or DEFAULT_QUERY_PARAMS
    return {
        **default_query_params,
        "page": str(page_number),
        "license": license_name,
    }


def _get_items(license_name):
    item_count = 0
    page_number = 1
    should_continue = True
    while should_continue:
        query_param = _get_query_params(
            license_name=license_name, page_number=page_number
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
        "title": media_data.get("name"),
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


preview_bitrates = {
    "review-hq-mp3": 128000,
    "preview-lq-mp3": 64000,
    "preview-hq-ogg": 192000,
    "preview-lq-ogg": 80000,
}


def _get_alt_files(media_data):
    alt_files = [
        {
            "url": media_data.get("download"),
            **_get_file_properties(media_data),
        }
    ]
    if previews := media_data.get("previews"):
        for preview_type in previews:
            preview_url = previews[preview_type]
            alt_files.append(
                {
                    "url": preview_url,
                    "filetype": preview_type.split("-")[-1],
                    "bit_rate": preview_bitrates[preview_type],
                }
            )
    return alt_files


def _get_file_properties(media_data):
    return {
        "bit_rate": int(media_data.get("bitrate")),
        "sample_rate": int(media_data.get("samplerate")),
        "filetype": media_data.get("type"),
        "filesize": media_data.get("filesize"),
    }


def _get_foreign_identifier(media_data):
    return media_data.get("id")


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
    if creator := item.get("username"):
        creator = creator.strip()
        creator_url = f"https://freesound.org/people/{creator}/"
    else:
        creator_url = None
    return creator, creator_url


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
        if field_value := item.get(field):
            metadata[field] = field_value
    return metadata


def _get_license(item):
    item_license_url = item.get("license")
    item_license = get_license_info(license_url=item_license_url)

    if item_license.license is None:
        return None
    return item_license


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s",
        level=logging.INFO,
    )
    logger = logging.getLogger(__name__)
    main()

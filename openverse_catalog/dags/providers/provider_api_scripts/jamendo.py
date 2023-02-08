"""
Content Provider:       Jamendo

ETL Process:            Use the API to identify all CC-licensed audio.

Output:                 TSV file containing the audio meta-data.

Notes:                  https://api.jamendo.com/v3.0/tracks/
                        35,000 requests per month for non-commercial apps
                        Jamendo Music has more than 500,000 tracks shared by
                        40,000 artists from over 150 countries all over
                        the world.
                        Audio quality: uploaded as WAV/ FLAC/ AIFF
                        bit depth: 16/24
                        sample rate: 44.1 or 48 kHz
                        channels: 1/2
"""
import logging
from urllib.parse import parse_qs, urlencode, urlsplit

import common
from airflow.models import Variable
from common import constants
from common.licenses import get_license_info
from common.loader import provider_details as prov
from common.urls import rewrite_redirected_url
from providers.provider_api_scripts.provider_data_ingester import ProviderDataIngester


logger = logging.getLogger(__name__)
logging.getLogger(common.urls.__name__).setLevel(logging.WARNING)


class JamendoDataIngester(ProviderDataIngester):
    providers = {"audio": prov.JAMENDO_DEFAULT_PROVIDER}
    endpoint = "https://api.jamendo.com/v3.0/tracks"
    batch_limit = 200
    headers = {"Accept": "application/json"}

    def get_media_type(self, record):
        return constants.AUDIO

    def get_next_query_params(self, prev_query_params, **kwargs):
        if not prev_query_params:
            # On first request, build default params.
            return {
                "format": "json",
                "client_id": Variable.get("API_KEY_JAMENDO"),
                "include": "musicinfo licenses stats lyrics",
                "imagesize": 200,
                "limit": self.batch_limit,
                "audioformat": "mp32",
                "offset": 0,
            }
        else:
            # Increment `offset` by the batch limit.
            return {
                **prev_query_params,
                "offset": prev_query_params["offset"] + self.batch_limit,
            }

    def get_batch_data(self, response_json):
        if response_json:
            return response_json.get("results")
        return None

    @staticmethod
    def _remove_param_from_url(url: str, param: str) -> str:
        """Remove a parameter from a provided URL."""
        parsed_url = urlsplit(url)
        query = parse_qs(parsed_url.query)
        query.pop(param, None)
        return parsed_url._replace(query=urlencode(query, doseq=True)).geturl()

    def _remove_trackid(self, thumbnail_url: str | None) -> str | None:
        """
        Remove the track ID from a URL.

        ``audio_set`` data is used to create a separate database table in the API.
        To make sure that any given ``audio_set`` appears in that table only once,
        all the fields for that ``audio_set`` need to have the same values. In
        Jamendo, the ``audio_set`` thumbnail has a ``trackid`` query parameter,
        which breaks this rule.
        This function removes the ``trackid`` query parameter from the URL to make
        all thumbnail values identical for an audio set.
        >>> base_url = "https://usercontent.jamendo.com"
        >>> url = f"{base_url}?type=album&id=119&width=200&trackid=732"
        >>> _remove_trackid(url)
        'https://usercontent.jamendo.com?type=album&id=119&width=200'
        """
        if thumbnail_url is None:
            return None
        return self._remove_param_from_url(thumbnail_url, "trackid")

    def _get_audio_url(self, data):
        """
        Parse out audio URL and remove the "from" parameter.

        Audio URLs have a "from" param which seems to encapsulate information about the
        calling application. Example from the API:
        https://prod-1.storage.jamendo.com/?trackid=1532771&format=mp31&from=app-devsite
        This information looks like an API key or secret when returned, so we remove it
        since it's not necessary for serving the audio files.
        >>> base_url = "https://prod-1.storage.jamendo.com/"
        >>> url = f"{base_url}?trackid=1532771&format=mp31&from=app-devsite"
        >>> _remove_param_from_url(url, "from")
        'https://prod-1.storage.jamendo.com/?trackid=1532771&format=mp31'
        :return: Tuple with main audio file information:
        - audio_url
        - duration (in milliseconds)
        """
        if (audio_url := data.get("audio")) is None:
            return None
        return self._remove_param_from_url(audio_url, "from")

    @staticmethod
    def _get_creator_data(data):
        base_url = "https://www.jamendo.com/artist/"
        if (creator_name := data.get("artist_name")) is None:
            return None, None

        creator_id = data.get("artist_id")
        creator_idstr = data.get("artist_idstr")
        if creator_id is not None and creator_idstr is not None:
            creator_url = f"{base_url}{creator_id}/{creator_idstr}"
        else:
            creator_url = None

        return creator_name.strip(), creator_url

    @staticmethod
    def _get_metadata(data):
        stats = data.get("stats", {})
        metadata = {
            "lyrics": data.get("lyrics") or None,
            "release_date": data.get("releasedate"),
            "downloads": stats.get("rate_download_total", 0),
            "listens": stats.get("rate_listened_total", 0),
            "playlists": stats.get("rate_playlisted_total", 0),
        }
        return {k: v for k, v in metadata.items() if v is not None}

    @staticmethod
    def _get_tags(data):
        tags = []

        musicinfo = data.get("musicinfo", {})
        if music_type := musicinfo.get("vocalinstrumental"):
            tags.append(music_type)
        if music_gender := musicinfo.get("gender"):
            tags.append(music_gender)
        if music_speed := musicinfo.get("speed"):
            tags.append(f"speed_{music_speed}")

        for tag_name in ["instruments", "vartags"]:
            tag_value = musicinfo.get("tags", {}).get(tag_name)
            if tag_value:
                tags.extend([tag for tag in tag_value if tag != "undefined"])

        return tags

    def get_record_data(self, data):
        if (foreign_identifier := data.get("id")) is None:
            return None

        if (foreign_landing_url := data.get("shareurl")) is None:
            return None

        if (audio_url := self._get_audio_url(data)) is None:
            return None

        license_url = data.get("license_ccurl")
        license_info = get_license_info(license_url=license_url)
        if license_info.license is None:
            return None

        duration = data.get("duration")
        if duration:
            duration = int(duration) * 1000
        title = data.get("name")
        thumbnail = data.get("image")
        genres = data.get("musicinfo", {}).get("tags", {}).get("genres")
        creator, creator_url = self._get_creator_data(data)
        metadata = self._get_metadata(data)
        tags = self._get_tags(data)
        # Jamendo only has music
        category = "music"
        # We request only mp32 (VBR) files
        filetype = "mp32"

        # Audio Set data
        set_url = None
        base_url = "https://www.jamendo.com/album/"
        audio_set = data.get("album_name")
        set_position = data.get("position")
        set_thumbnail = self._remove_trackid(data.get("album_image"))
        set_id = data.get("album_id")
        if set_id and audio_set:
            set_slug = (
                audio_set.lower().replace(" ", "-").replace("/", "-").replace("--", "")
            )
            set_url = rewrite_redirected_url(f"{base_url}{set_id}/{set_slug}")
        return {
            "title": title,
            "creator": creator,
            "creator_url": creator_url,
            "foreign_identifier": foreign_identifier,
            "foreign_landing_url": foreign_landing_url,
            "audio_url": audio_url,
            "duration": duration,
            "filetype": filetype,
            "thumbnail_url": thumbnail,
            "license_info": license_info,
            "meta_data": metadata,
            "raw_tags": tags,
            "category": category,
            "genres": genres,
            "set_foreign_id": set_id,
            "audio_set": audio_set,
            "set_position": set_position,
            "set_url": set_url,
            "set_thumbnail": set_thumbnail,
        }


def main():
    logger.info("Begin: Jamendo data ingestion.")
    ingester = JamendoDataIngester()
    ingester.ingest_records()


if __name__ == "__main__":
    main()

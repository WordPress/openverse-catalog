from collections import namedtuple
import logging

from common.storage import columns
from common.storage.media import (
    MediaStore,
)


logger = logging.getLogger(__name__)

AUDIO_TSV_COLUMNS = [
    # The order of this list maps to the order of the columns in the TSV.
    columns.StringColumn(
        name="foreign_identifier", required=False, size=3000, truncate=False
    ),
    columns.URLColumn(name="foreign_landing_url", required=True, size=1000),
    columns.URLColumn(
        # `url` in DB
        name="audio_url",
        required=True,
        size=3000,
    ),
    columns.URLColumn(
        # `thumbnail` in DB
        name="thumbnail_url",
        required=False,
        size=3000,
    ),
    columns.IntegerColumn(name="duration", required=False),
    columns.IntegerColumn(name="filesize", required=False),
    columns.StringColumn(name="license_", required=True, size=50, truncate=False),
    columns.StringColumn(
        name="license_version", required=True, size=25, truncate=False
    ),
    columns.StringColumn(name="creator", required=False, size=2000, truncate=True),
    columns.URLColumn(name="creator_url", required=False, size=2000),
    columns.StringColumn(name="title", required=False, size=5000, truncate=True),
    columns.JSONColumn(name="meta_data", required=False),
    columns.JSONColumn(name="tags", required=False),
    columns.StringColumn(name="provider", required=False, size=80, truncate=False),
    columns.StringColumn(name="source", required=False, size=80, truncate=False),
]

Audio = namedtuple("Audio", [c.NAME for c in AUDIO_TSV_COLUMNS])


class AudioStore(MediaStore):
    """
    A class that stores audio information from a given provider.

    Optional init arguments:
    provider:       String marking the provider in the `audio` table of the DB.
    output_file:    String giving a temporary .tsv filename (*not* the
                    full path) where the audio info should be stored.
    output_dir:     String giving a path where `output_file` should be placed.
    buffer_length:  Integer giving the maximum number of audio information rows
                    to store in memory before writing them to disk.
    """

    def __init__(
        self,
        provider=None,
        output_file=None,
        output_dir=None,
        buffer_length=100,
        media_type="audio",
        tsv_columns=None,
    ):
        super().__init__(provider, output_file, output_dir, buffer_length, media_type)
        self.columns = tsv_columns if tsv_columns is not None else AUDIO_TSV_COLUMNS

    def add_item(
            self,
            foreign_landing_url=None,
            audio_url=None,
            thumbnail_url=None,
            license_url=None,
            license_=None,
            license_version=None,
            foreign_identifier=None,
            duration=None,
            creator=None,
            creator_url=None,
            title=None,
            meta_data=None,
            raw_tags=None,
            source=None
    ):

        """
        Add information for a single audio to the AudioStore.

        Required Arguments:

        foreign_landing_url:  URL of page where the audio lives on the
                              source website.
        audio_url:            Direct link to the audio file

        Semi-Required Arguments

        license_url:      URL of the license for the audio on the
                          Creative Commons website.
        license_:         String representation of a Creative Commons
                          license.  For valid options, see
                          `common.license.constants.get_license_path_map()`
        license_version:  Version of the given license.  In the case of
                          the `publicdomain` license, which has no
                          version, one should pass
                          `common.license.constants.NO_VERSION` here.

        Note on license arguments: These are 'semi-required' in that
        either a valid `license_url` must be given, or a valid
        `license_`, `license_version` pair must be given. Otherwise, the
        audio data will be discarded.

        Optional Arguments:
        thumbnail_url:       Direct link to a thumbnail image for the audio
        foreign_identifier:  Unique identifier for the audio on the
                             source site.
        duration:            in seconds
        creator:             The creator of the audio.
        creator_url:         The user page, or home page of the creator.
        title:               Title of the audio.
        meta_data:           Dictionary of meta_data about the audio.
                             Currently, a key that we prefer to have is
                             `description`. If 'license_url' is included
                             in this dictionary, and `license_url` is
                             given as an argument, the argument will
                             replace the one given in the dictionary.
        raw_tags:            List of tags associated with the audio.
        source:              If different from the provider.  This might
                             be the case when we get information from
                             some aggregation of audios.  In this case,
                             the `source` argument gives the aggregator,
                             and the `provider` argument in the
                             AudioStore init function is the specific
                             provider of the audio.
        """
        audio = self._get_audio(
            foreign_landing_url=foreign_landing_url,
            audio_url=audio_url,
            thumbnail_url=thumbnail_url,
            license_url=license_url,
            license_=license_,
            license_version=license_version,
            foreign_identifier=foreign_identifier,
            duration=duration,
            creator=creator,
            creator_url=creator_url,
            title=title,
            meta_data=meta_data,
            raw_tags=raw_tags,
            source=source
        )
        self.save_item(audio)
        return self._total_items

    def _get_audio(
            self,
            foreign_identifier,
            foreign_landing_url,
            audio_url,
            thumbnail_url,
            duration,
            license_url,
            license_,
            license_version,
            creator,
            creator_url,
            title,
            meta_data,
            raw_tags,
            source,
    ):
        valid_license_info, raw_license_url = \
            self.get_valid_license_info(license_url, license_, license_version)
        source, meta_data, tags = self.parse_item_metadata(
            valid_license_info.url,
            raw_license_url,
            source,
            meta_data,
            raw_tags
        )
        return Audio(
            foreign_identifier=foreign_identifier,
            foreign_landing_url=foreign_landing_url,
            audio_url=audio_url,
            thumbnail_url=thumbnail_url,
            license_=license_info.license,
            license_version=license_info.version,
            filesize=None,
            creator=creator,
            creator_url=creator_url,
            title=title,
            meta_data=meta_data,
            tags=tags,
            provider=self._PROVIDER,
            source=source,
            duration=duration,
        )


class MockAudioStore(AudioStore):
    """
    A class that mocks the role of the AudioStore class. This class replaces
    all functionality of AudioStore that calls the internet.

    For information about all arguments other than license_info refer to
    AudioStore class.

    Required init arguments:
    license_info:       A named tuple consisting of valid license info from
                        the test script in which MockAudioStore is being used.
    """

    def __init__(
        self,
        provider=None,
        output_file=None,
        output_dir=None,
        buffer_length=100,
        license_info=None,
    ):
        super().__init__(provider=provider)
        self.license_info = license_info

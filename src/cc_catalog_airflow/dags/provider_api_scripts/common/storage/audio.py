from collections import namedtuple
from datetime import datetime
import logging
import os

import copy
from common.licenses import licenses
from common.storage import util
from common.storage import columns
from common.storage.media import (
    MEDIA_TSV_COLUMNS,
    MediaStore,
)


logger = logging.getLogger(__name__)

AUDIO_TSV_COLUMNS = copy.deepcopy(MEDIA_TSV_COLUMNS)
AUDIO_TSV_COLUMNS.extend(
    [
        # The order of this list maps to the order of the columns in the TSV.
        columns.IntegerColumn(name="duration", required=False)
    ]
)

Audio = namedtuple("Audio", [c.NAME for c in AUDIO_TSV_COLUMNS])


class AudioStore(MediaStore):
    """
    A class that stores image information from a given provider.

    Optional init arguments:
    provider:       String marking the provider in the `audio` table of the DB.
    output_file:    String giving a temporary .tsv filename (*not* the
                    full path) where the audio info should be stored.
    output_dir:     String giving a path where `output_file` should be placed.
    buffer_length:  Integer giving the maximum number of image information rows
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

    def _get_media(self, **kwargs):
        kwargs["provider"] = self._PROVIDER

        return Audio(
            **kwargs,
        )


class MockAudioStore(AudioStore):
    """
    A class that mocks the role of the ImageStore class. This class replaces
    all functionality of ImageStore that calls the internet.

    For information about all arguments other than license_info refer to
    ImageStore class.

    Required init arguments:
    license_info:       A named tuple consisting of valid license info from
                        the test script in which MockImageStore is being used.
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

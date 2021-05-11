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

IMAGE_TSV_COLUMNS = copy.deepcopy(MEDIA_TSV_COLUMNS)
IMAGE_TSV_COLUMNS.extend(
    [
        # The order of this list maps to the order of the columns in the TSV.
        columns.IntegerColumn(name="width", required=False),
        columns.IntegerColumn(name="height", required=False),
        columns.BooleanColumn(name="watermarked", required=False),
    ]
)

Image = namedtuple("Image", [c.NAME for c in IMAGE_TSV_COLUMNS])


class ImageStore(MediaStore):
    """
    A class that stores image information from a given provider.

    Optional init arguments:
    provider:       String marking the provider in the `image` table of the DB.
    output_file:    String giving a temporary .tsv filename (*not* the
                    full path) where the image info should be stored.
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
        media_type="image",
        tsv_columns=None,
    ):
        super().__init__(provider, output_file, output_dir, buffer_length, media_type)
        self.columns = tsv_columns if tsv_columns is not None else IMAGE_TSV_COLUMNS

    def _get_media(self, **kwargs):
        """
        Add information for a single image to the ImageStore.

        Required Arguments:

        foreign_landing_url:  URL of page where the image lives on the
                              source website.
        image_url:            Direct link to the image file

        Semi-Required Arguments

        license_url:      URL of the license for the image on the
                          Creative Commons website.
        license_:         String representation of a Creative Commons
                          license.  For valid options, see
                          `common.license.constants.get_license_path_map()`
        license_version:  Version of the given license.  In the case of
                          the `publicdomain` license, which has no
                          version, one shoud pass
                          `common.license.constants.NO_VERSION` here.

        Note on license arguments: These are 'semi-required' in that
        either a valid `license_url` must be given, or a valid
        `license_`, `license_version` pair must be given. Otherwise, the
        image data will be discarded.

        Optional Arguments:

        thumbnail_url:       Direct link to a thumbnail-sized version of
                             the image
        foreign_identifier:  Unique identifier for the image on the
                             source site.
        width:               in pixels
        height:              in pixels
        creator:             The creator of the image.
        creator_url:         The user page, or home page of the creator.
        title:               Title of the image.
        meta_data:           Dictionary of meta_data about the image.
                             Currently, a key that we prefer to have is
                             `description`. If 'license_url' is included
                             in this dictionary, and `license_url` is
                             given as an argument, the argument will
                             replace the one given in the dictionary.
        raw_tags:            List of tags associated with the image
        watermarked:         A boolean, or 't' or 'f' string; whether or
                             not the image has a noticeable watermark.
        source:              If different from the provider.  This might
                             be the case when we get information from
                             some aggregation of images.  In this case,
                             the `source` argument gives the aggregator,
                             and the `provider` argument in the
                             ImageStore init function is the specific
                             provider of the image.
        """
        kwargs["provider"] = self._PROVIDER
        return Image(
            **kwargs,
        )


class MockImageStore(ImageStore):
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

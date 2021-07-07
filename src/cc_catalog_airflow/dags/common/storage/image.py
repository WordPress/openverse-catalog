from collections import namedtuple
import logging
from typing import Optional, Dict, Union

from common.storage import columns
from common.storage.media import MediaStore
from common import LicenseInfo

logger = logging.getLogger(__name__)

IMAGE_TSV_COLUMNS = [
    # The order of this list maps to the order of the columns in the TSV.
    columns.StringColumn(
        name='foreign_identifier', required=False, size=3000, truncate=False
    ),
    columns.URLColumn(
        name='foreign_landing_url', required=True, size=1000
    ),
    columns.URLColumn(
        # `url` in DB
        name='image_url', required=True, size=3000
    ),
    columns.URLColumn(
        # `thumbnail` in DB
        name='thumbnail_url', required=False, size=3000
    ),
    columns.IntegerColumn(
        name='width', required=False
    ),
    columns.IntegerColumn(
        name='height', required=False
    ),
    columns.IntegerColumn(
        name='filesize', required=False
    ),
    columns.StringColumn(
        name='license_', required=True, size=50, truncate=False
    ),
    columns.StringColumn(
        name='license_version', required=True, size=25, truncate=False
    ),
    columns.StringColumn(
        name='creator', required=False, size=2000, truncate=True
    ),
    columns.URLColumn(
        name='creator_url', required=False, size=2000
    ),
    columns.StringColumn(
        name='title', required=False, size=5000, truncate=True
    ),
    columns.JSONColumn(
        name='meta_data', required=False
    ),
    columns.JSONColumn(
        name='tags', required=False
    ),
    columns.BooleanColumn(
        name='watermarked', required=False
    ),
    columns.StringColumn(
        name='provider', required=False, size=80, truncate=False
    ),
    columns.StringColumn(
        name='source', required=False, size=80, truncate=False
    ),
    columns.StringColumn(
        name="ingestion_type", required=False, size=80, truncate=False
    ),
]

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
        super().__init__(
            provider, output_file, output_dir, buffer_length, media_type
        )
        self.columns = IMAGE_TSV_COLUMNS \
            if tsv_columns is None else tsv_columns

    def add_item(
        self,
        foreign_landing_url: str,
        image_url: str,
        thumbnail_url: Optional[str] = None,
        license_info: Optional[LicenseInfo] = None,
        foreign_identifier: Optional[str] = None,
        width: Optional[int] = None,
        height: Optional[int] = None,
        creator: Optional[str] = None,
        creator_url: Optional[str] = None,
        title: Optional[str] = None,
        meta_data: Optional[Union[Dict, str]] = None,
        raw_tags=None,
        watermarked: Optional[str] = "f",
        source: Optional[str] = None,
        ingestion_type: Optional[str] = None,
    ):
        """
        Add information for a single image to the ImageStore.

        Required Arguments:
        foreign_landing_url:  URL of page where the image lives on the
                              source website.
        image_url:            Direct link to the image file

        Semi-Required Arguments
        license_info:         An object of type LicenseInfo, created using
                              either:
                              1.
                               - the string representation of an Open License
                               (For valid options, see
                               `common.license.constants.get_license_path_map()`)
                               AND
                               - string with license version. In the case of
                               the `publicdomain` license, which has no
                               version, one should pass
                               `common.license.constants.NO_VERSION` here

                               AND / OR

                               2.
                                - license url.

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
        ingestion_type:      String showing how the image was ingested:
                             through an api - 'provider_api' or using the
                             commoncrawl database - 'commoncrawl'
        """
        if license_info.license is None:
            logger.warning("Invalid license provided")
            return None
        image_data = {
            'foreign_landing_url': foreign_landing_url,
            'image_url': image_url,
            'thumbnail_url': thumbnail_url,
            'license_info': license_info,
            'foreign_identifier': foreign_identifier,
            'width': width,
            'height': height,
            'creator': creator,
            'creator_url': creator_url,
            'title': title,
            'meta_data': meta_data,
            'raw_tags': raw_tags,
            'watermarked': watermarked,
            'source': source,
            'ingestion_type': ingestion_type
        }
        image = self._get_image(**image_data)
        if image is not None:
            self.save_item(image)
        return self.total_items

    def _get_image(self, **kwargs) -> Optional[Image]:
        """Validates image information and returns Image namedtuple"""
        image_metadata = self.clean_media_metadata(**kwargs)
        if image_metadata is None:
            return None

        return Image(**image_metadata)


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
        logger.info(f"Initialized with provider {provider}")
        super().__init__(provider=provider)
        self.license_info = license_info

from collections import namedtuple
from datetime import datetime
import logging
import os

from common.licenses import licenses
from common.storage import util
from common.storage import columns

logger = logging.getLogger(__name__)

MEDIA_TSV_COLUMNS = [
    # The order of this list maps to the order of the columns in the TSV.
    columns.StringColumn(
        name="foreign_identifier", required=False, size=3000, truncate=False
    ),
    columns.URLColumn(name="foreign_landing_url", required=True, size=1000),
    columns.URLColumn(
        # `url` in DB
        name="media_url",
        required=True,
        size=3000,
    ),
    columns.URLColumn(
        # `thumbnail` in DB
        name="thumbnail_url",
        required=False,
        size=3000,
    ),
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

Media = namedtuple("Image", [c.NAME for c in MEDIA_TSV_COLUMNS])

# Filter out tags that exactly match these terms. All terms should be
# lowercase.
TAG_BLACKLIST = {"no person", "squareformat"}

# Filter out tags that contain the following terms. All entrees should be
# lowercase.
TAG_CONTAINS_BLACKLIST = {
    "flickriosapp",
    "uploaded",
    ":",
    "=",
    "cc0",
    "by",
    "by-nc",
    "by-nd",
    "by-sa",
    "by-nc-nd",
    "by-nc-sa",
    "pdm",
}


class MediaStore:
    """
    A class that stores media information from a given provider.

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
        media_type="generic",
    ):
        logger.info(f"Initialized {media_type} media store with provider {provider}")
        self.media_type = media_type
        self._media_buffer = []
        self._total_items = 0
        self._PROVIDER = provider
        self._BUFFER_LENGTH = buffer_length
        self._NOW = datetime.now()
        self._OUTPUT_PATH = self._initialize_output_path(
            output_dir,
            output_file,
            provider,
        )
        self.columns = None

    def save_item(self, media) -> None:
        tsv_row = self._create_tsv_row(media)
        if tsv_row:
            self._media_buffer.append(tsv_row)
            self._total_items += 1
        if len(self._media_buffer) >= self._BUFFER_LENGTH:
            self._flush_buffer()

    def add_item(
        self,
        **kwargs,
    ) -> int:
        """
        Add information for a single media item to the MediaStore.

        Required Arguments:

        foreign_landing_url:  URL of page where the image lives on the
                              source website.
        media_url:            Direct link to the image file

        Semi-Required Arguments

        license_url:      URL of the license for the image on the
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
        valid_license_info = licenses.get_license_info(
            license_url=kwargs["license_url"],
            license_=kwargs["license_"],
            license_version=kwargs["license_version"],
        )
        source = util.get_source(kwargs["source"], self._PROVIDER)
        meta_data = self._enrich_meta_data(
            kwargs["meta_data"],
            license_url=valid_license_info.url,
            raw_license_url=kwargs["license_url"],
        )
        tags = self._enrich_tags(kwargs["raw_tags"])
        media = self._get_media(
            tags=tags,
            meta_data=meta_data,
            source=source,
            valid_license_info=valid_license_info,
            **kwargs,
        )
        self.save_item(media)
        return self._total_items

    def commit(self):
        """Writes all remaining images in the buffer to disk."""
        self._flush_buffer()

        return self._total_items

    def _initialize_output_path(self, output_dir, output_file, provider):
        if output_dir is None:
            logger.info(
                "No given output directory.  " "Using OUTPUT_DIR from environment."
            )
            output_dir = os.getenv("OUTPUT_DIR")
        if output_dir is None:
            logger.warning(
                "OUTPUT_DIR is not set in the environment.  " "Output will go to /tmp."
            )
            output_dir = "/tmp"

        if output_file is not None:
            output_file = str(output_file)
        else:
            output_file = (
                f'{provider}_{self.media_type}_{datetime.strftime(self._NOW, "%Y%m%d%H%M%S")}'
                f".tsv"
            )

        output_path = os.path.join(output_dir, output_file)
        logger.info(f"Output path: {output_path}")
        return output_path

    def _get_total_items(self):
        return self._total_items

    """Get total images for directly using in scripts."""
    total_media_items = property(_get_total_items)

    def _get_media(self, **kwargs):
        raise NotImplementedError("Implement get media method")

    def _create_tsv_row(
        self,
        image,
    ):
        if self.columns is None:
            self.columns = MEDIA_TSV_COLUMNS
        row_length = len(columns)
        prepared_strings = [
            columns[i].prepare_string(image[i]) for i in range(row_length)
        ]
        logger.debug(f"Prepared strings list:\n{prepared_strings}")
        for i in range(row_length):
            if columns[i].REQUIRED and prepared_strings[i] is None:
                logger.warning(f"Row missing required {columns[i].NAME}")
                return None
        else:
            return (
                "\t".join([s if s is not None else "\\N" for s in prepared_strings])
                + "\n"
            )

    def _flush_buffer(self):
        buffer_length = len(self._image_buffer)
        if buffer_length > 0:
            logger.info(f"Writing {buffer_length} lines from buffer to disk.")
            with open(self._OUTPUT_PATH, "a") as f:
                f.writelines(self._image_buffer)
                self._image_buffer = []
                logger.debug(f"Total Images Processed so far:  {self._total_images}")
        else:
            logger.debug("Empty buffer!  Nothing to write.")
        return buffer_length

    @staticmethod
    def _tag_blacklisted(tag):
        """
        Tag is banned or contains a banned substring.
        :param tag: the tag to be verified against the blacklist
        :return: true if tag is blacklisted, else returns false
        """
        if type(tag) == dict:  # check if the tag is already enriched
            tag = tag.get("name")
        if tag in TAG_BLACKLIST:
            return True
        for blacklisted_substring in TAG_CONTAINS_BLACKLIST:
            if blacklisted_substring in tag:
                return True
        return False

    @staticmethod
    def _enrich_meta_data(meta_data, license_url, raw_license_url):
        if type(meta_data) != dict:
            logger.debug(f"`meta_data` is not a dictionary: {meta_data}")
            enriched_meta_data = {
                "license_url": license_url,
                "raw_license_url": raw_license_url,
            }
        else:
            enriched_meta_data = meta_data
            enriched_meta_data.update(
                license_url=license_url, raw_license_url=raw_license_url
            )
        return enriched_meta_data

    def _enrich_tags(self, raw_tags):
        if type(raw_tags) != list:
            logger.debug("`tags` is not a list.")
            return None
        else:
            return [
                self._format_raw_tag(tag)
                for tag in raw_tags
                if not self._tag_blacklisted(tag)
            ]

    def _format_raw_tag(self, tag):
        if type(tag) == dict and tag.get("name") and tag.get("provider"):
            logger.debug(f"Tag already enriched: {tag}")
            return tag
        else:
            logger.debug(f"Enriching tag: {tag}")
            return {"name": tag, "provider": self._PROVIDER}

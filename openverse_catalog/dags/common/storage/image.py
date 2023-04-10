import logging
from collections import namedtuple
from dataclasses import dataclass

from common.licenses import LicenseInfo
from common.storage.media import MediaStore
from common.storage.tsv_columns import CURRENT_IMAGE_TSV_COLUMNS


logger = logging.getLogger(__name__)

Image = namedtuple("Image", [c.name for c in CURRENT_IMAGE_TSV_COLUMNS])


@dataclass
class ProviderImageData:
    """
    A class that stores the image data collected from the provider.
    The data is then processed by the MediaStore class.

    Required parameters are `foreign_landing_url`, `image_url`, `license_info`.
    Image data without the required parameters will be discarded.

    @param foreign_landing_url: URL of page where the image lives on the source website.
    ## Selection criteria
    The URL should be the direct link and not have redirects, if possible.

    @param image_url: Direct link to the image file.
    ## Selection criteria
    This is a URL from which the image can be downloaded directly. It should not
    have any query parameters if possible. Some providers, like SMK, have very large
    images (TODO: how much is too much?)
    In this case, it is necessary to also save the `thumbnail_url` for showing the
    image on the frontend.

    @param license_info: LicenseInfo object that has
        - the URL of the license for the image,
        - string representation of the license,
        - version of the license,
        - raw license URL that was by provider, if different from canonical URL
    ## Selection criteria
    Openverse accepts the 6 CC licenses, the `public domain` mark and the `CC0` mark.
    There are several versions of each license.
    For valid options of licenses (the combination of the license name and
    license variant),
    see `common.license.constants.get_license_path_map()`.
    To get the LicenseInfo object, use `get_license_info` with either
    (license_ and license_version) or (license_url) named parameters.
    In the case of the `publicdomain` license, which has no version, one should pass
    `common.license.constants.NO_VERSION` here.
    The license is taken from the provider API response.

    @param thumbnail_url: URL of the image's thumbnail. This is used for the
    image preview on the frontend.
    ## Selection criteria
    Since the thumbnails are used to on the search page, and when the main
    image is being loaded by the frontend, thumbnail needs to be of a good quality.
     While there's no strict criteria for the thumbnail, it should be at least
     400px wide.

    @param filesize: Size of the image file in bytes.
    ## Selection criteria
     TODO: Add details on what to do for invalid file sizes.


    @param filetype: eg. 'jpg', 'svg' # TODO: Add a list of valid file types. Add
    details on validation (jpg -> jpeg, etc.). If the image filetype is in the URL
    extension, then we don't need to pass it here. Instead, the MediaStore class
    will extract the filetype.

    @param foreign_identifier: The unique identifier of the image on the provider's
    website.
    Often, this will be a part of the image's foreign_landing_url.

    @param width: Width of the image in pixels. Sometimes, the width can be extracted by
    sending a head request for the image's direct `url`. However, this can be imprecise.

    @param height: Height of the image in pixels. Sometimes, the height can be
    extracted by sending a head request for the image's direct `url`. However, this
    can be imprecise.

    @param creator: Name of the creator of the image.
    ## Selection criteria
    This can be the name of the photographer,
    the artist, etc. Sometimes, it can be the name of the maker of
    the object in the image.

    @param creator_url: URL of the creator's website

    @param title: The title of the image.

    @param meta_data: A dictionary of additional metadata. Details such as a
    full-text description, the date the image was created, geographical
    location of the object or a photo, etc. can be stored here.

    @param raw_tags: A list of strings (tags associated with the image on the
    provider site).

    @param category: The category of the image. This is one of the values in the
    `category` Enum.
    # TODO: Add details on how to validate the category.
     defaults to the default category for the provider from
     `openverse_catalog/dags/common/loader/provider_details.py`.

    @param watermarked: Whether the image is watermarked. It is often not
    possible to determine
    this for each individual image, so we only set this value to True if the provider
    watermarks all their images. # TODO: check if this is true.

    @param source: The name of the source on the provider's website. Some providers are
    a collection of different organizations. For example, the Smithsonian has many
    different museums. In this case, the `source` is the name of the individual museum.
    Other providers host images from multiple sources. For example, the Flickr hosts
    images by NASA. In this case, `source` is NASA, and `provider` is Flickr.
    In all other cases, the `source` is the same as the `provider`.

    @param ingestion_type: The type of ingestion # TODO: this should be an Enum.
    """

    foreign_landing_url: str
    image_url: str
    license_info: LicenseInfo
    thumbnail_url: str
    width: int
    height: int
    filesize: int
    filetype: str
    foreign_identifier: str
    creator: str
    creator_url: str
    title: str
    meta_data: dict
    raw_tags: list
    category: str
    watermarked: bool
    source: str
    ingestion_type: str


class ImageStore(MediaStore):
    """
    A class that stores image information from a given provider.

    Optional init arguments:
    provider:       String marking the provider in the `image` table of the DB.
    tsv_suffix:     Optional string to append to the tsv filename.
    output_file:    String giving a temporary .tsv filename (*not* the
                    full path) where the image info should be stored.
    output_dir:     String giving a path where `output_file` should be placed.
    buffer_length:  Integer giving the maximum number of image information rows
                    to store in memory before writing them to disk.
    """

    def __init__(
        self,
        provider=None,
        tsv_suffix=None,
        output_file=None,
        output_dir=None,
        buffer_length=100,
        media_type="image",
        tsv_columns=None,
        strip_url_trailing_slashes: bool = True,
    ):
        super().__init__(
            provider, tsv_suffix, buffer_length, media_type, strip_url_trailing_slashes
        )
        self.columns = CURRENT_IMAGE_TSV_COLUMNS if tsv_columns is None else tsv_columns

    def add_item(
        self,
        foreign_landing_url: str,
        image_url: str,
        license_info: LicenseInfo,
        thumbnail_url: str | None = None,
        filesize: int | None = None,
        filetype: str | None = None,
        foreign_identifier: str | None = None,
        width: int | None = None,
        height: int | None = None,
        creator: str | None = None,
        creator_url: str | None = None,
        title: str | None = None,
        meta_data: dict | str | None = None,
        raw_tags=None,
        category: str | None = None,
        watermarked: str | None = "f",
        source: str | None = None,
        ingestion_type: str | None = None,
        **kwargs,
    ):
        """
        Add information for a single image to the ImageStore.

        Required Arguments:
        foreign_landing_url:  URL of page where the image lives on the
                              source website.
        image_url:            Direct link to the image file

        license_info:         LicenseInfo object that has
                              - the URL of the license for the image,
                              - string representation of the license,
                              - version of the license,
                              - raw license URL that was by provider,
                                if different from canonical URL
        For valid options of license names, see
        `common.license.constants.get_license_path_map()`.

        To get the LicenseInfo object, use `get_license_info` with either
        (license_ and license_version) or (license_url) named parameters.
        In the case of the `publicdomain` license, which has no version,
        one should pass `common.license.constants.NO_VERSION` here.

        Image data without the required parameters will be discarded.

        Optional Arguments:

        thumbnail_url:       Direct link to a thumbnail-sized version of
                             the image.
        filesize:            Size of the image file in bytes.
        filetype:            eg. 'jpg', 'svg'.
        foreign_identifier:  Unique identifier for the image on the
                             source site.
        width:               in pixels.
        height:              in pixels.
        creator:             The creator of the image.
        creator_url:         The user page, or home page of the creator.
        title:               Title of the image.
        meta_data:           Dictionary of meta_data about the image.
                             Currently, a key that we prefer to have is
                             `description`. If 'license_url' is included
                             in this dictionary, and `license_url` is
                             given as an argument, the argument will
                             replace the one given in the dictionary.
        raw_tags:            List of tags associated with the image.
        category:            The image category, defaults to the default
                             category for the provider from
                             common/loader/provider_details.py.
        watermarked:         A boolean, or 't' or 'f' string; whether
                             the image has a noticeable watermark.
        source:              If different from the provider.  This might
                             be the case when we get information from
                             some aggregation of images.  In this case,
                             the `source` argument gives the aggregator,
                             and the `provider` argument in the
                             ImageStore init function is the specific
                             provider of the image.
        ingestion_type:      Set programmatically
        """

        image_data = {
            "foreign_landing_url": foreign_landing_url,
            "image_url": image_url,
            "thumbnail_url": thumbnail_url,
            "filesize": filesize,
            "filetype": filetype,
            "license_info": license_info,
            "foreign_identifier": foreign_identifier,
            "width": width,
            "height": height,
            "creator": creator,
            "creator_url": creator_url,
            "title": title,
            "meta_data": meta_data,
            "raw_tags": raw_tags,
            "category": category,
            "watermarked": watermarked,
            "source": source,
            "ingestion_type": ingestion_type,
        }
        image = self._get_image(**image_data)
        if image is not None:
            self.save_item(image)
        return self.total_items

    def _get_image(self, **kwargs) -> Image | None:
        """Validate image information and return Image namedtuple."""
        image_metadata = self.clean_media_metadata(**kwargs)
        if image_metadata is None:
            return None
        # Convert the `image_url` key used in ImageStore, TSV and
        # provider API scripts into `url` key used in db
        image_metadata["url"] = image_metadata.pop("image_url")
        return Image(**image_metadata)


class MockImageStore(ImageStore):
    """
    A class that mocks the role of the ImageStore class.

    This class replaces all functionality of ImageStore that calls the internet.
    It also allows for easy introspection into the images added to the
    media_buffer by making it a public attribute, and not converting the
    images to TSV.

    For information about all arguments other than license_info refer to
    ImageStore class.

    Required init arguments:
    license_info:       A named tuple consisting of valid license info from
                        the test script in which MockImageStore is being used.
    """

    NULLABLE_FIELDS = [
        "thumbnail_url",
        "filesize",
        "filetype",
        "foreign_identifier",
        "width",
        "height",
        "creator",
        "creator_url",
        "title",
        "meta_data",
        "raw_tags",
        "category",
        "watermarked",
        "source",
        "ingestion_type",
    ]

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
        self.media_buffer = []

    def add_item(self, **kwargs):
        image_data = kwargs
        for field in MockImageStore.NULLABLE_FIELDS:
            if field not in image_data:
                image_data[field] = None
        image = self._get_image(**image_data)
        if image is not None:
            self.media_buffer.append(image)
        return len(self.media_buffer)

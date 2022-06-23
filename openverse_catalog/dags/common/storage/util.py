"""
This module has public methods which are useful for storage operations.
"""
import logging
from typing import Type

from common.storage.audio import AudioStore
from common.storage.image import ImageStore
from common.storage.media import MediaStore


logger = logging.getLogger(__name__)

MEDIA_STORE_MAPPING = {
    "image": ImageStore,
    "audio": AudioStore,
}


def get_media_store_class(media_type: str) -> Type[MediaStore]:
    StoreClass = MEDIA_STORE_MAPPING.get(media_type)
    if StoreClass is None:
        raise ValueError(f"No MediaStore is configured for type: {media_type}")
    return StoreClass


def get_source(source, provider):
    """
    Returns `source` if given, otherwise `provider`
    """
    if not source:
        source = provider

    return source

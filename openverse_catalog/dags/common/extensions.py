from typing import Literal


EXTENSIONS = {
    "image": {"jpg", "jpeg", "png", "gif", "bmp", "webp", "tiff", "tif", "svg", "webp"},
    "audio": {"mp3", "ogg", "wav", "aiff", "flac", "aac", "m4a", "wma", "mp4", "m4b"},
}


def extract_filetype(url: str, media_type: Literal["image", "audio"]) -> str | None:
    """
    Extracts the filetype from a media url extension.
    """
    possible_filetype = url.split(".")[-1]
    if possible_filetype in EXTENSIONS[media_type]:
        return possible_filetype
    return None

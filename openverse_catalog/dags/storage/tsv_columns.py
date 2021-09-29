from typing import List

from storage import columns as col
from storage.columns import Column
from util.constants import AUDIO, IMAGE


# Image has 'legacy' 000 version
# Audio versions start at 001
CURRENT_VERSION = {
    AUDIO: "001",
    IMAGE: "001",
}

COLUMNS = {
    AUDIO: {
        "001": [
            # The order of this list maps to the order of the columns in the TSV.
            col.FOREIGN_ID_COLUMN,
            col.LANDING_URL_COLUMN,
            col.DIRECT_URL_COLUMN,
            col.THUMBNAIL_COLUMN,
            col.FILETYPE_COLUMN,
            col.FILESIZE_COLUMN,
            col.LICENSE_COLUMN,
            col.LICENSE_VERSION_COLUMN,
            col.CREATOR_COLUMN,
            col.CREATOR_URL_COLUMN,
            col.TITLE_COLUMN,
            col.META_DATA_COLUMN,
            col.TAGS_COLUMN,
            col.CATEGORY_COLUMN,
            col.WATERMARKED_COLUMN,
            col.PROVIDER_COLUMN,
            col.SOURCE_COLUMN,
            col.INGESTION_TYPE_COLUMN,
            col.DURATION_COLUMN,
            col.BIT_RATE_COLUMN,
            col.SAMPLE_RATE_COLUMN,
            col.GENRES_COLUMN,
            col.AUDIO_SET_COLUMN,
            col.SET_POSITION_COLUMN,
            col.ALT_FILES_COLUMN,
        ],
    },
    IMAGE: {
        # Legacy columns with `ingestion_type` column
        "000": [
            col.FOREIGN_ID_COLUMN,
            col.LANDING_URL_COLUMN,
            col.DIRECT_URL_COLUMN,
            col.THUMBNAIL_COLUMN,
            col.WIDTH_COLUMN,
            col.HEIGHT_COLUMN,
            col.FILESIZE_COLUMN,
            col.LICENSE_COLUMN,
            col.LICENSE_VERSION_COLUMN,
            col.CREATOR_COLUMN,
            col.CREATOR_URL_COLUMN,
            col.TITLE_COLUMN,
            col.META_DATA_COLUMN,
            col.TAGS_COLUMN,
            col.WATERMARKED_COLUMN,
            col.PROVIDER_COLUMN,
            col.SOURCE_COLUMN,
            col.INGESTION_TYPE_COLUMN,
        ],
        "001": [
            col.FOREIGN_ID_COLUMN,
            col.LANDING_URL_COLUMN,
            col.DIRECT_URL_COLUMN,
            col.THUMBNAIL_COLUMN,
            col.FILETYPE_COLUMN,
            col.FILESIZE_COLUMN,
            col.LICENSE_COLUMN,
            col.LICENSE_VERSION_COLUMN,
            col.CREATOR_COLUMN,
            col.CREATOR_URL_COLUMN,
            col.TITLE_COLUMN,
            col.META_DATA_COLUMN,
            col.TAGS_COLUMN,
            col.CATEGORY_COLUMN,
            col.WATERMARKED_COLUMN,
            col.PROVIDER_COLUMN,
            col.SOURCE_COLUMN,
            col.INGESTION_TYPE_COLUMN,
            col.WIDTH_COLUMN,
            col.HEIGHT_COLUMN,
        ],
    },
}

AUDIO_TSV_COLUMNS: List[Column] = COLUMNS[AUDIO][CURRENT_VERSION[AUDIO]]
IMAGE_TSV_COLUMNS: List[Column] = COLUMNS[IMAGE][CURRENT_VERSION[IMAGE]]

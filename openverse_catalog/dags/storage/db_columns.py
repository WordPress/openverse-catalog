"""
This module contains the lists of database columns
in the same order as in the main image / audio databases.
"""
from storage import columns as col


# Columns that are only in the main table;
# not in the TSVs or loading table
NOT_IN_LOADING_TABLE = {
    col.IDENTIFIER_COLUMN,
    col.CREATED_ON_COLUMN,
    col.UPDATED_ON_COLUMN,
    col.LAST_SYNCED_COLUMN,
    col.REMOVED_COLUMN,
}

# The list of columns in main db table in the same order
IMAGE_TABLE_COLUMNS = [
    col.IDENTIFIER_COLUMN,
    col.CREATED_ON_COLUMN,
    col.UPDATED_ON_COLUMN,
    col.INGESTION_TYPE_COLUMN,
    col.PROVIDER_COLUMN,
    col.SOURCE_COLUMN,
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
    col.LAST_SYNCED_COLUMN,
    col.REMOVED_COLUMN,
    col.FILETYPE_COLUMN,
]

AUDIO_TABLE_COLUMNS = [
    col.IDENTIFIER_COLUMN,
    col.CREATED_ON_COLUMN,
    col.UPDATED_ON_COLUMN,
    col.INGESTION_TYPE_COLUMN,
    col.PROVIDER_COLUMN,
    col.SOURCE_COLUMN,
    col.FOREIGN_ID_COLUMN,
    col.LANDING_URL_COLUMN,
    col.DIRECT_URL_COLUMN,
    col.THUMBNAIL_COLUMN,
    col.FILESIZE_COLUMN,
    col.LICENSE_COLUMN,
    col.LICENSE_VERSION_COLUMN,
    col.CREATOR_COLUMN,
    col.CREATOR_URL_COLUMN,
    col.TITLE_COLUMN,
    col.META_DATA_COLUMN,
    col.TAGS_COLUMN,
    col.WATERMARKED_COLUMN,
    col.LAST_SYNCED_COLUMN,
    col.REMOVED_COLUMN,
    col.DURATION_COLUMN,
    col.BIT_RATE_COLUMN,
    col.SAMPLE_RATE_COLUMN,
    col.CATEGORY_COLUMN,
    col.GENRES_COLUMN,
    col.AUDIO_SET_COLUMN,
    col.SET_POSITION_COLUMN,
    col.ALT_FILES_COLUMN,
    col.FILETYPE_COLUMN,
]

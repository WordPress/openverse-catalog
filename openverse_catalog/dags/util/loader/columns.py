"""
This file holds string constants for the column names in the image
database, as well as the loading tables in the PostgreSQL DB.
"""
import logging
from enum import Enum, auto
from typing import List, NewType, Optional

from util.constants import AUDIO, IMAGE
from util.loader import column_names as col


NOW = "NOW()"
FALSE = "'f'"


class Datatype(Enum):
    bool = "boolean"
    char = "character"
    int = "integer"
    jsonb = "jsonb"
    timestamp = "timestamp with time zone"


class UpsertStrategy(Enum):
    now = auto()
    false = auto()
    newest_non_null = auto()
    merge_jsonb_objects = auto()
    merge_jsonb_arrays = auto()
    merge_array = auto()


Datatypes = NewType("Datatype", Datatype)
UpsertStrategies = NewType("UpsertStrategy", UpsertStrategy)


def _newest_non_null(column: str) -> str:
    return f"{column} = COALESCE(EXCLUDED.{column}, old.{column})"


def _merge_jsonb_objects(column: str) -> str:
    """
    This function returns SQL that merges the top-level keys of the
    a JSONB column, taking the newest available non-null value.
    """
    return f"""{column} = COALESCE(
           jsonb_strip_nulls(old.{column})
             || jsonb_strip_nulls(EXCLUDED.{column}),
           EXCLUDED.{column},
           old.{column}
         )"""


def _merge_jsonb_arrays(column: str) -> str:
    return f"""{column} = COALESCE(
           (
             SELECT jsonb_agg(DISTINCT x)
             FROM jsonb_array_elements(old.{column} || EXCLUDED.{column}) t(x)
           ),
           EXCLUDED.{column},
           old.{column}
         )"""


def _merge_array(column: str) -> str:
    return f"""{column} = COALESCE(
       (
         SELECT array_agg(DISTINCT x)
         FROM unnest(old.{column} || EXCLUDED.{column}) t(x)
       ),
       EXCLUDED.{column},
       old.{column}
       )"""


def _now(column):
    return f"{column} = {NOW}"


def _false(column):
    return f"{column} = {FALSE}"


class DbColumn:
    """Class for columns in the database,
    including conflict resolution for new data
    """

    def __init__(
        self,
        name: str,
        datatype: Datatype,
        constraint: Optional[str] = None,
        upsert_strategy: UpsertStrategy = UpsertStrategy.newest_non_null,
    ):
        """
        :param name: column name in the db, may be different from TSV column name
        (eg. 'audio_url' in TSV is 'url' in db)
        :param datatype: one of the Datatype enum values,
        the column datatype in the db table
        :param constraint: column constraint for Postgres db
        :param upsert_strategy: what to do when some new data for a media file
        is received: one of the UpsertStrategy enum values.
        For simple data types, the newest non null value is chosen
        JSON objects and arrays are merged so that newest values are written
        """
        self.name = name
        self.datatype = datatype
        self.constraint = constraint
        self.upsert_strategy = upsert_strategy

    @property
    def upsert_name(self):
        if self.upsert_strategy == UpsertStrategy.now:
            return NOW
        elif self.upsert_strategy == UpsertStrategy.false:
            return FALSE
        else:
            return self.name

    @property
    def upsert_value(self):
        strategy = {
            UpsertStrategy.newest_non_null: _newest_non_null,
            UpsertStrategy.now: _now,
            UpsertStrategy.false: _false,
            UpsertStrategy.merge_jsonb_objects: _merge_jsonb_objects,
            UpsertStrategy.merge_jsonb_arrays: _merge_jsonb_arrays,
            UpsertStrategy.merge_array: _merge_array,
        }.get(self.upsert_strategy)
        if strategy is None:
            logging.warning(f"Unrecognized column {self.name}; ignoring during upsert")
            return ""
        else:
            return strategy(self.name)


# The dictionary of all columns in the Postgres databases, both `image` and `audio`
DB_COLUMNS = {
    col.IDENTIFIER: DbColumn("identifier", Datatype.char, "varying(3000)"),
    col.FOREIGN_ID: DbColumn("foreign_identifier", Datatype.char, "varying(3000)"),
    col.LANDING_URL: DbColumn("foreign_landing_url", Datatype.char, "varying(1000)"),
    col.DIRECT_URL: DbColumn("url", Datatype.char, "varying(3000)"),
    col.THUMBNAIL: DbColumn("thumbnail", Datatype.char, "varying(3000)"),
    col.WIDTH: DbColumn("width", Datatype.int),
    col.HEIGHT: DbColumn("height", Datatype.int),
    col.FILESIZE: DbColumn("filesize", Datatype.int),
    col.LICENSE: DbColumn("license", Datatype.char, "varying(50)"),
    col.LICENSE_VERSION: DbColumn("license_version", Datatype.char, "varying(25)"),
    col.CREATOR: DbColumn("creator", Datatype.char, "varying(2000)"),
    col.CREATOR_URL: DbColumn("creator_url", Datatype.char, "varying(2000)"),
    col.TITLE: DbColumn("title", Datatype.char, "varying(5000)"),
    col.META_DATA: DbColumn(
        "meta_data", Datatype.jsonb, None, UpsertStrategy.merge_jsonb_objects
    ),
    col.TAGS: DbColumn("tags", Datatype.jsonb, None, UpsertStrategy.merge_jsonb_arrays),
    col.WATERMARKED: DbColumn("watermarked", Datatype.bool),
    col.PROVIDER: DbColumn("provider", Datatype.char, "varying(80)"),
    col.SOURCE: DbColumn("source", Datatype.char, "varying(80)"),
    col.INGESTION_TYPE: DbColumn("ingestion_type", Datatype.char, "varying(80)"),
    col.CREATED_ON: DbColumn(
        "created_on", Datatype.timestamp, "NOT NULL", UpsertStrategy.now
    ),
    col.UPDATED_ON: DbColumn(
        "updated_on", Datatype.timestamp, "NOT NULL", UpsertStrategy.now
    ),
    col.LAST_SYNCED: DbColumn(
        "last_synced_with_source", Datatype.timestamp, None, UpsertStrategy.now
    ),
    col.REMOVED: DbColumn(
        "removed_from_source", Datatype.char, "varying(3000)", UpsertStrategy.false
    ),
    col.DURATION: DbColumn("duration", Datatype.int),
    col.BIT_RATE: DbColumn("bit_rate", Datatype.int),
    col.SAMPLE_RATE: DbColumn("sample_rate", Datatype.int),
    col.CATEGORY: DbColumn("category", Datatype.char, "varying(100)"),
    col.GENRES: DbColumn(
        "genres", Datatype.char, "varying(80)[]", UpsertStrategy.merge_array
    ),
    col.AUDIO_SET: DbColumn(
        "audio_set", Datatype.jsonb, None, UpsertStrategy.merge_jsonb_objects
    ),
    col.ALT_FILES: DbColumn(
        "alt_files", Datatype.jsonb, None, UpsertStrategy.merge_jsonb_objects
    ),
}

# Dictionary of lists of column names common for all media type,
# for the intermediary loading table, and the main `image` or `audio` table
# in the Upstream (also called Openledger db)
common_columns = {
    "loading": [
        col.FOREIGN_ID,
        col.LANDING_URL,
        col.DIRECT_URL,
        col.THUMBNAIL,
        col.FILESIZE,
        col.LICENSE,
        col.LICENSE_VERSION,
        col.CREATOR,
        col.CREATOR_URL,
        col.TITLE,
        col.META_DATA,
        col.TAGS,
        col.WATERMARKED,
        col.PROVIDER,
        col.SOURCE,
        col.INGESTION_TYPE,
    ],
    "main": [
        # IDENTIFIER,
        col.CREATED_ON,
        col.UPDATED_ON,
        col.INGESTION_TYPE,
        col.PROVIDER,
        col.SOURCE,
        col.FOREIGN_ID,
        col.LANDING_URL,
        col.DIRECT_URL,
        col.THUMBNAIL,
        col.FILESIZE,
        col.LICENSE,
        col.LICENSE_VERSION,
        col.CREATOR,
        col.CREATOR_URL,
        col.TITLE,
        col.LAST_SYNCED,
        col.REMOVED,
        col.META_DATA,
        col.TAGS,
        col.WATERMARKED,
        # LAST_SYNCED
        # REMOVED
    ],
}

# Dictionary of lists of column names that are specific to the media type.
# These columns are at the end of the db table.
media_columns = {
    AUDIO: [
        col.DURATION,
        col.BIT_RATE,
        col.SAMPLE_RATE,
        col.CATEGORY,
        col.GENRES,
        col.AUDIO_SET,
        col.ALT_FILES,
    ],
    IMAGE: [col.WIDTH, col.HEIGHT],
}


def get_table_columns(media_type: str, table_type: str = "main") -> List[str]:
    """
    Returns the list of column names that a loading or main db table
    for the media type should have.
    :param media_type: One of the supported media types, from
    `util/constants.py`
    :param table_type: Whether it is intermediary loading table, or the main
    `image` or `audio` table.
    :return: list of strings - names of the columns for selected db table
    """
    if table_type not in {"main", "loading"}:
        raise TypeError(f"Cannot create table of type {table_type}")
    common_db_columns = common_columns[table_type]
    media_specific_columns = media_columns.get(media_type)
    if media_specific_columns is None:
        raise TypeError(f"Cannot get loading table columns for type {media_type}")

    return common_db_columns + media_specific_columns


def create_column_definitions(column_list: List[str]) -> str:
    """
    Turns a list of db column names into a string
    used to create the db table, with the correct datatype and
    constraint.
    >>> create_column_definitions(['identifier', 'width'])
    identifier character varying(3000), width integer

    Silently skips if column is not in the ALL_DB_COLUMNS
    :param column_list: list of column names, from ALL_DB_COLUMNS
    :return: A string of column characteristics joined with a comma
    """
    column_definitions = []
    for column_name in column_list:
        column = DB_COLUMNS.get(column_name)
        if column is None:
            continue
        dt = column.datatype.value
        constraint = column.constraint
        if constraint is not None:
            constraint = f" {constraint}"
        else:
            constraint = ""
        column_string = f"{column_name} {dt}{constraint}"
        column_definitions.append(column_string)
    return ",\n  ".join(column_definitions)


column_names = DB_COLUMNS.keys()

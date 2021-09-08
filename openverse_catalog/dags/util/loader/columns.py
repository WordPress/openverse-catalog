"""
This file holds string constants for the column names in the image
database, as well as the loading tables in the PostgreSQL DB.
"""
import logging
from enum import Enum, auto
from typing import List, NewType, Optional

from util.constants import AUDIO, IMAGE


NOW = "NOW()"
FALSE = "'f'"

IDENTIFIER = "identifier"
FOREIGN_ID = "foreign_identifier"
LANDING_URL = "foreign_landing_url"
DIRECT_URL = "url"
THUMBNAIL = "thumbnail"
WIDTH = "width"
HEIGHT = "height"
FILESIZE = "filesize"
LICENSE = "license"
LICENSE_VERSION = "license_version"
CREATOR = "creator"
CREATOR_URL = "creator_url"
TITLE = "title"
META_DATA = "meta_data"
TAGS = "tags"
WATERMARKED = "watermarked"
PROVIDER = "provider"
SOURCE = "source"
INGESTION_TYPE = "ingestion_type"
CREATED_ON = "created_on"
UPDATED_ON = "updated_on"
LAST_SYNCED = "last_synced_with_source"
REMOVED = "removed_from_source"
DURATION = "duration"
BIT_RATE = "bit_rate"
SAMPLE_RATE = "sample_rate"
CATEGORY = "category"
GENRES = "genres"
AUDIO_SET = "audio_set"
ALT_FILES = "alt_files"


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
    return f"{column} = NOW()"


def _false(column):
    return f"{column} = 'f'"


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
        :param datatype: one of the Datatype enum values
        :param constraint: column constraint for Postgres db
        :param upsert_strategy: what to do when some new data for a media file
        is received: one of the UpsertStrategy enum values.
        For simple datatypes, the newest non null value is chosen
        JSON objects and arrays are merged, and on conflict
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


DB_COLUMNS = {
    IDENTIFIER: DbColumn("identifier", Datatype.char, "varying(3000)"),
    FOREIGN_ID: DbColumn("foreign_identifier", Datatype.char, "varying(3000)"),
    LANDING_URL: DbColumn("foreign_landing_url", Datatype.char, "varying(1000)"),
    DIRECT_URL: DbColumn("url", Datatype.char, "varying(3000)"),
    THUMBNAIL: DbColumn("thumbnail", Datatype.char, "varying(3000)"),
    WIDTH: DbColumn("width", Datatype.int),
    HEIGHT: DbColumn("height", Datatype.int),
    FILESIZE: DbColumn("filesize", Datatype.int),
    LICENSE: DbColumn("license", Datatype.char, "varying(50)"),
    LICENSE_VERSION: DbColumn("license_version", Datatype.char, "varying(25)"),
    CREATOR: DbColumn("creator", Datatype.char, "varying(2000)"),
    CREATOR_URL: DbColumn("creator_url", Datatype.char, "varying(2000)"),
    TITLE: DbColumn("title", Datatype.char, "varying(5000)"),
    META_DATA: DbColumn(
        "meta_data", Datatype.jsonb, None, UpsertStrategy.merge_jsonb_objects
    ),
    TAGS: DbColumn("tags", Datatype.jsonb, None, UpsertStrategy.merge_jsonb_arrays),
    WATERMARKED: DbColumn("watermarked", Datatype.bool),
    PROVIDER: DbColumn("provider", Datatype.char, "varying(80)"),
    SOURCE: DbColumn("source", Datatype.char, "varying(80)"),
    INGESTION_TYPE: DbColumn("ingestion_type", Datatype.char, "varying(80)"),
    CREATED_ON: DbColumn(
        "created_on", Datatype.timestamp, "NOT NULL", UpsertStrategy.now
    ),
    UPDATED_ON: DbColumn(
        "updated_on", Datatype.timestamp, "NOT NULL", UpsertStrategy.now
    ),
    LAST_SYNCED: DbColumn(
        "last_synced_with_source", Datatype.timestamp, None, UpsertStrategy.now
    ),
    REMOVED: DbColumn(
        "removed_from_source", Datatype.char, "varying(3000)", UpsertStrategy.false
    ),
    DURATION: DbColumn("duration", Datatype.int),
    BIT_RATE: DbColumn("bit_rate", Datatype.int),
    SAMPLE_RATE: DbColumn("sample_rate", Datatype.int),
    CATEGORY: DbColumn("category", Datatype.char, "varying(100)"),
    GENRES: DbColumn(
        "genres", Datatype.char, "varying(80)[]", UpsertStrategy.merge_array
    ),
    AUDIO_SET: DbColumn(
        "audio_set", Datatype.jsonb, None, UpsertStrategy.merge_jsonb_objects
    ),
    ALT_FILES: DbColumn(
        "alt_files", Datatype.jsonb, None, UpsertStrategy.merge_jsonb_objects
    ),
}

common_columns = {
    "loading": [
        FOREIGN_ID,
        LANDING_URL,
        DIRECT_URL,
        THUMBNAIL,
        FILESIZE,
        LICENSE,
        LICENSE_VERSION,
        CREATOR,
        CREATOR_URL,
        TITLE,
        META_DATA,
        TAGS,
        WATERMARKED,
        PROVIDER,
        SOURCE,
        INGESTION_TYPE,
    ],
    "main": [
        # IDENTIFIER,
        CREATED_ON,
        UPDATED_ON,
        INGESTION_TYPE,
        PROVIDER,
        SOURCE,
        FOREIGN_ID,
        LANDING_URL,
        DIRECT_URL,
        THUMBNAIL,
        FILESIZE,
        LICENSE,
        LICENSE_VERSION,
        CREATOR,
        CREATOR_URL,
        TITLE,
        LAST_SYNCED,
        REMOVED,
        META_DATA,
        TAGS,
        WATERMARKED,
        # LAST_SYNCED
        # REMOVED
    ],
}

media_columns = {
    AUDIO: [
        DURATION,
        BIT_RATE,
        SAMPLE_RATE,
        CATEGORY,
        GENRES,
        AUDIO_SET,
        ALT_FILES,
    ],
    IMAGE: [WIDTH, HEIGHT],
}


def get_table_columns(media_type: str, table_type: str = "main") -> List[str]:
    if table_type not in ["main", "loading"]:
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
    return ",\n".join(column_definitions)


column_names = DB_COLUMNS.keys()

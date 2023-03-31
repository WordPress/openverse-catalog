import json
import logging
from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import NewType

from common.storage.columns_docs import (
    alt_files_description,
    audio_set_description,
    bit_rate_description,
    category_description,
    created_on_description,
    creator_description,
    creator_url_description,
    direct_url_description,
    duration_description,
    filesize_description,
    filetype_description,
    foreign_id_description,
    genres_description,
    height_description,
    ingestion_type_description,
    landing_url_description,
    last_synced_description,
    license_description,
    license_version_description,
    meta_data_description,
    provider_description,
    removed_description,
    sample_rate_description,
    set_position_description,
    source_description,
    tags_description,
    thumbnail_url_description,
    title_description,
    updated_on_description,
    watermarked_description,
    width_description,
)


logger = logging.getLogger(__name__)

NOW = "NOW()"
FALSE = "'f'"
NULL = "NULL"


class Datatype(Enum):
    bool = "boolean"
    char = "character"
    int = "integer"
    jsonb = "jsonb"
    timestamp = "timestamp with time zone"
    uuid = "uuid"


class UpsertStrategy(Enum):
    now = auto()
    false = auto()
    newest_non_null = auto()
    merge_jsonb_objects = auto()
    merge_jsonb_arrays = auto()
    merge_array = auto()
    no_change = ()


Datatypes = NewType("Datatype", Datatype)
UpsertStrategies = NewType("UpsertStrategy", UpsertStrategy)


def _newest_non_null(column: str) -> str:
    return f"{column} = COALESCE(EXCLUDED.{column}, old.{column})"


def _merge_jsonb_objects(column: str) -> str:
    """
    Return an SQL that merges the top-level keys of a JSONB column.
    This takes the newest available non-null value.
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


def _now(column: str):
    return f"{column} = {NOW}"


def _false(column):
    return f"{column} = {FALSE}"


class Column(ABC):
    """
    Implementations of this base class represent columns in a PostgreSQL DB.

    Each implementation must implement a `prepare_string` method to
    properly format data for a given column type.
    """

    strategies = {
        UpsertStrategy.newest_non_null: _newest_non_null,
        UpsertStrategy.now: _now,
        UpsertStrategy.false: _false,
        UpsertStrategy.merge_jsonb_objects: _merge_jsonb_objects,
        UpsertStrategy.merge_jsonb_arrays: _merge_jsonb_arrays,
        UpsertStrategy.merge_array: _merge_array,
    }

    def __init__(
        self,
        name: str,
        required: bool,
        description: str,
        datatype: Datatype = Datatype.char,
        upsert_strategy: UpsertStrategy | None = UpsertStrategy.newest_non_null,
        constraint: str | None = None,
        db_name: str | None = None,
        nullable: bool | None = None,
    ):
        """
        Initialize a column.

        :param name: The column name used in TSV, ImageStore and provider API scripts,
        can be different from the name in the database.
        :param required: If True, the database column will be set to 'NOT NULL'
        :param description: A description of the column used to generate documentation.
        :param datatype: Postgres datatype representation
        :param upsert_strategy: Shows the strategy used when the data for a media item
        is re-ingested: Simple values are replaced with newer non-null values,
        json and array values are merged, some timestamps are set to the execution time.
        :param constraint: Column constraint in database
        :param db_name: Column name in database, if different from TSV name
        """
        self.name = name
        self.required = required
        self.description = description
        self.datatype = datatype
        self.upsert_strategy = upsert_strategy
        self.constraint = constraint
        self.db_name = db_name or name
        self.nullable = nullable if nullable is not None else not required

    def __str__(self):
        return f"{type(self).__name__} {self.name}"

    @abstractmethod
    def prepare_string(self, value):
        """
        Return a string to be imported to the corresponding field in the DB.

        Return Nonetype if the eventual column in the DB should be Null.
        """
        pass

    def __sanitize_string(self, data):
        if data is None:
            return None
        else:
            # We join a split string because it removes all whitespace
            # characters
            return " ".join(
                str(data)
                .replace('"', "'")
                .replace("\b", "")
                .replace("\\", "\\\\")
                .split()
            )

    def __enforce_char_limit(self, string, limit, truncate=True):
        if not type(string) == str:
            logger.debug(
                f"Cannot limit characters on non-string type {type(string)}."
                f"Input was {string}."
            )
            return None
        if len(string) > limit:
            logger.warning(f"String over char limit of {limit}.  Input was {string}.")
            return string[:limit] if truncate else None
        else:
            return string

    @property
    def upsert_name(self):
        if self.upsert_strategy == UpsertStrategy.now:
            return NOW
        elif self.upsert_strategy == UpsertStrategy.false:
            return FALSE
        else:
            return self.db_name

    @property
    def upsert_value(self):
        strategy = Column.strategies.get(self.upsert_strategy)
        if strategy is None:
            logging.warning(
                f"Unrecognized column {self.name}; setting to NULL during upsert"
            )
            return NULL
        else:
            return strategy(self.db_name)

    def create_definition(self, is_loading: bool):
        dt = self.datatype.value
        constraint = "" if self.constraint is None else f" {self.constraint}"
        nullable = ""
        if not is_loading and not self.nullable:
            nullable = " NOT NULL"
        return f"{self.db_name} {dt}{constraint}{nullable}"


class IntegerColumn(Column):
    """
    Represents a PostgreSQL column of type integer.

    name:      name of the corresponding column in the DB
    required:  whether the column should be considered required by the
               instantiating script.  (Not necessarily mapping to
               `not null` columns in the PostgreSQL table)
    """

    def __init__(
        self,
        name: str,
        required: bool,
        description: str,
        constraint: str | None = None,
        db_name: str | None = None,
    ):
        super().__init__(
            name,
            required,
            description,
            datatype=Datatype.int,
            upsert_strategy=UpsertStrategy.newest_non_null,
            constraint=constraint,
            db_name=db_name,
        )

    def prepare_string(self, value):
        """
        Return a string representation to the best integer approx of input.

        If there is no sane mapping from the input to an integer,
        returns None.

        value: for useful output this should be reasonably castable to an int.
        """
        try:
            number = str(int(float(value)))
        except (TypeError, ValueError) as e:
            logger.debug(f"input {value} is not castable to an int.  The error was {e}")
            number = None
        return number


class BooleanColumn(Column):
    """
    Represents a PostgreSQL column of type boolean.

    name:      name of the corresponding column in the DB
    required:  whether the column should be considered required by the
               instantiating script.  (Not necessarily mapping to
               `not null` columns in the PostgreSQL table)
    """

    def __init__(
        self,
        name: str,
        required: bool,
        description: str,
        upsert_strategy: UpsertStrategy | None = UpsertStrategy.newest_non_null,
        constraint: str | None = None,
        db_name: str | None = None,
    ):
        super().__init__(
            name,
            required,
            description,
            datatype=Datatype.bool,
            upsert_strategy=upsert_strategy,
            constraint=constraint,
            db_name=db_name,
        )
        self.constraint = constraint

    def prepare_string(self, value):
        """
        Return a string `t` or `f`, as appropriate to input.

        If there is no sane mapping from the input to a boolean,
        returns None.

        value: for useful output this should be reasonably castable to a bool.
        """
        bool_map = {
            "t": [True, "true", "True", "t", "T"],
            "f": [False, "false", "False", "f", "F"],
        }
        for tf in bool_map:
            if value in bool_map[tf]:
                return tf
        logger.debug(f"{value} is not a valid PostgreSQL bool")
        return None


class JSONColumn(Column):
    """
    Represents a PostgreSQL column of type jsonb.

    name:      name of the corresponding column in the DB
    required:  whether the column should be considered required by the
               instantiating script.  (Not necessarily mapping to
               `not null` columns in the PostgreSQL table)
    """

    def __init__(
        self,
        name: str,
        required: bool,
        description: str,
        db_name: str | None = None,
        upsert_strategy: UpsertStrategy | None = None,
    ):
        strategy = upsert_strategy or UpsertStrategy.merge_jsonb_objects
        super().__init__(
            name,
            required,
            description,
            datatype=Datatype.jsonb,
            upsert_strategy=strategy,
            db_name=db_name,
            constraint=None,
        )

    def prepare_string(self, value):
        """
        Return a json string as appropriate to input.

        Also sanitizes values within the json to ensure they are
        loadable into a PostgreSQL table.

        If given empty input, returns None.

        value: lists and dicts will be turned into json,
               other input will be turned into sanitized strings.
        """
        sanitized_json = self._sanitize_json_values(value)
        return (
            json.dumps(sanitized_json, ensure_ascii=False) if sanitized_json else None
        )

    def _sanitize_json_values(self, value, recursion_limit=100):
        """
        Recursively sanitize the non-dict/non-list values of an input dict or list in
        preparation for dumping to a JSON string.
        """
        input_type = type(value)

        if value is None:
            return value
        elif input_type not in [dict, list] or recursion_limit <= 0:
            return self._Column__sanitize_string(value)
        elif input_type == list:
            return [
                self._sanitize_json_values(item, recursion_limit=recursion_limit - 1)
                for item in value
            ]
        else:
            return {
                key: self._sanitize_json_values(
                    val, recursion_limit=recursion_limit - 1
                )
                for key, val in value.items()
            }


class StringColumn(Column):
    """
    Represents a PostgreSQL column of type varchar.

    name:      name of the corresponding column in the DB
    required:  whether the column should be considered required by the
               instantiating script.  (Not necessarily mapping to
               `not null` columns in the PostgreSQL table)
    size:      width of the varchar in the table.  Erring on the small
               side is fine, but setting this value larger than the
               width of the corresponding column in the table is not
               recommended.
    truncate:  Whether or not it's acceptable to truncate the input to
               fit within the required size.  If not, input over the
               limit will be mapped to None.
    """

    def __init__(
        self,
        name: str,
        required: bool,
        description: str,
        size: int,
        truncate: bool,
        db_name: str | None = None,
    ):
        self.SIZE = size
        self.TRUNCATE = truncate
        super().__init__(
            name,
            required,
            description,
            datatype=Datatype.char,
            upsert_strategy=UpsertStrategy.newest_non_null,
            constraint=f"varying({size})",
            db_name=db_name,
        )

    def prepare_string(self, value):
        """Sanitizes input and enforces the character limit, returning a string."""
        return self._Column__enforce_char_limit(
            self._Column__sanitize_string(value), self.SIZE, self.TRUNCATE
        )


class UUIDColumn(Column):
    """
    Represents the PrimaryKey `identifier` column in PostgreSQL.

    name:          Column name
    """

    def __init__(self, name: str):
        super().__init__(
            name,
            required=True,
            description="Unique identifier (UUID) for each media item, "
            "created when the item is saved to the loading table.",
            datatype=Datatype.uuid,
            upsert_strategy=None,
            constraint="PRIMARY KEY DEFAULT public.uuid_generate_v4()",
        )

    def prepare_string(self, value):
        return value


class TimestampColumn(Column):
    """
    Represents a PostgreSQL column of type `timestamp with time zone`.

    name:             Column name
    required:         If True, `NOT NULL` constraint is added
    upsert_strategy:  Strategy to use for data for a media item is re-ingested,
                      one of the UpsertStrategy. Default is to replace with `NOW()`
    """

    def __init__(
        self,
        name: str,
        required: bool,
        description: str,
        upsert_strategy: UpsertStrategy | None = UpsertStrategy.now,
    ):
        super().__init__(
            name,
            required,
            description,
            datatype=Datatype.timestamp,
            upsert_strategy=upsert_strategy,
        )

    @property
    def upsert_name(self):
        return NOW

    def prepare_string(self, value):
        return value


class URLColumn(Column):
    """
    Represents a PostgreSQL column of type varchar, which should hold a URL.

    name:      name of the corresponding column in the DB
    required:  whether the column should be considered required by the
               instantiating script.  (Not necessarily mapping to
               `not null` columns in the PostgreSQL table)
    size:      width of the varchar in the table.  Erring on the small
               side is fine, but setting this value larger than the
               width of the corresponding column in the table is not
               recommended.

    Note:  Different from StringColumn in that we *never* truncate a URL
           string.
    """

    def __init__(
        self,
        name: str,
        required: bool,
        description: str,
        size: int,
        nullable: bool = False,
        db_name: str | None = None,
    ):
        self.SIZE = size
        super().__init__(
            name,
            required,
            description,
            datatype=Datatype.char,
            upsert_strategy=UpsertStrategy.newest_non_null,
            constraint=f"varying({size})",
            db_name=db_name,
            nullable=nullable,
        )

    def prepare_string(self, value):
        """
        Return input unchanged, as long as it is a valid URL string.

        Also enforces the character limit of the column. If the input
        value fails a validation, returns None.
        """
        if self._Column__sanitize_string(value) != value:
            return None
        else:
            return self._Column__enforce_char_limit(value, self.SIZE, False)


class ArrayColumn(Column):
    """
    Represents a PostgreSQL column of type Array.

    Arrays should hold elements of the given base_column type.

    name:           name of the corresponding column in the DB
    required:       whether the column should be considered required by the
                    instantiating script.  (Not necessarily mapping to
                    `not null` columns in the PostgreSQL table)
    base_column:    type of the elements in the array, another column
    """

    def __init__(
        self,
        name: str,
        required: bool,
        description: str,
        base_column: Column,
        db_name: str | None = None,
    ):
        self.base_column = base_column
        super().__init__(
            name,
            required,
            description,
            datatype=Datatype.char,
            upsert_strategy=UpsertStrategy.merge_array,
            constraint="varying(80)[]",
            db_name=db_name,
        )

    def prepare_string(self, value):
        """
        Return a string representation of an array.

        The format in PostgreSQL is: `{<item 1>, <item 2>...}`.
        Apply changes and validations of the corresponding base column type.
        """
        input_type = type(value)

        if value is None:
            return value
        elif input_type != list:
            arr_str = self.base_column.prepare_string(value)
            return "{" + arr_str + "}" if arr_str else None

        values = []
        for val in value:
            if val is None:
                values.append(None)
            else:
                values.append(self.base_column.prepare_string(val))
        arr_str = json.dumps(values, ensure_ascii=False)
        return "{" + arr_str[1:-1] + "}" if arr_str else None


FOREIGN_ID = StringColumn(
    name="foreign_identifier",
    required=True,
    size=3000,
    truncate=False,
    description=foreign_id_description,
)
LANDING_URL = URLColumn(
    name="foreign_landing_url",
    required=True,
    size=1000,
    nullable=True,
    description=landing_url_description,
)
DIRECT_URL = URLColumn(
    # `url` in DB
    name="url",
    required=True,
    size=3000,
    db_name="url",
    description=direct_url_description,
)
THUMBNAIL = URLColumn(
    # `thumbnail` in DB
    name="thumbnail_url",
    required=False,
    size=3000,
    db_name="thumbnail",
    description=thumbnail_url_description,
)
FILESIZE = IntegerColumn(
    name="filesize", required=False, description=filesize_description
)
LICENSE = StringColumn(
    name="license_",
    required=True,
    size=50,
    truncate=False,
    db_name="license",
    description=license_description,
)
LICENSE_VERSION = StringColumn(
    name="license_version",
    required=True,
    size=25,
    truncate=False,
    description=license_version_description,
)
CREATOR = StringColumn(
    name="creator",
    required=False,
    size=2000,
    truncate=True,
    description=creator_description,
)
CREATOR_URL = URLColumn(
    name="creator_url", required=False, size=2000, description=creator_url_description
)
TITLE = StringColumn(
    name="title",
    required=False,
    size=5000,
    truncate=True,
    description=title_description,
)
META_DATA = JSONColumn(
    name="meta_data", required=False, description=meta_data_description
)
TAGS = JSONColumn(
    name="tags",
    required=False,
    upsert_strategy=UpsertStrategy.merge_jsonb_arrays,
    description=tags_description,
)
WATERMARKED = BooleanColumn(
    name="watermarked", required=False, description=watermarked_description
)
PROVIDER = StringColumn(
    name="provider",
    required=False,
    size=80,
    truncate=False,
    description=provider_description,
)
SOURCE = StringColumn(
    name="source",
    required=False,
    size=80,
    truncate=False,
    description=source_description,
)
INGESTION_TYPE = StringColumn(
    name="ingestion_type",
    required=False,
    size=80,
    truncate=False,
    description=ingestion_type_description,
)
WIDTH = IntegerColumn(name="width", required=False, description=width_description)
HEIGHT = IntegerColumn(name="height", required=False, description=height_description)

DURATION = IntegerColumn(
    name="duration", required=False, description=duration_description
)
BIT_RATE = IntegerColumn(
    name="bit_rate",
    required=False,
    description=bit_rate_description,
)

SAMPLE_RATE = IntegerColumn(
    name="sample_rate",
    required=False,
    description=sample_rate_description,
)
CATEGORY = StringColumn(
    name="category",
    required=False,
    size=80,
    truncate=False,
    description=category_description,
)
GENRES = ArrayColumn(
    name="genres",
    required=False,
    base_column=StringColumn(name="genre", required=False, size=80, truncate=False),
    description=genres_description,
)
AUDIO_SET = JSONColumn(
    # set name, thumbnail, url, identifier etc.
    name="audio_set",
    required=False,
    description=audio_set_description,
)
SET_POSITION = IntegerColumn(
    name="set_position",
    required=False,
    description=set_position_description,
)
ALT_FILES = JSONColumn(
    # Alternative files: url, filesize, bit_rate, sample_rate
    name="alt_files",
    required=False,
    upsert_strategy=UpsertStrategy.merge_jsonb_arrays,
    description=alt_files_description,
)

IDENTIFIER = UUIDColumn(
    name="identifier",
)

CREATED_ON = TimestampColumn(
    name="created_on",
    required=True,
    upsert_strategy=UpsertStrategy.no_change,
    description=created_on_description,
)

UPDATED_ON = TimestampColumn(
    name="updated_on",
    required=True,
    description=updated_on_description,
)

LAST_SYNCED = TimestampColumn(
    name="last_synced_with_source", required=False, description=last_synced_description
)

REMOVED = BooleanColumn(
    name="removed_from_source",
    required=True,
    upsert_strategy=UpsertStrategy.false,
    description=removed_description,
)

FILETYPE = StringColumn(
    name="filetype",
    required=False,
    truncate=False,
    size=5,
    description=filetype_description,
)

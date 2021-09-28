import json
import logging
from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import NewType, Optional

from common import urls


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
        datatype: Datatype = Datatype.char,
        upsert_strategy: Optional[UpsertStrategy] = UpsertStrategy.newest_non_null,
        constraint: Optional[str] = None,
        db_name: Optional[str] = None,
        nullable: Optional[bool] = None,
    ):
        """
        :param name: The column name used in TSV, ImageStore and provider API scripts,
        can be different from the name in the database.
        :param required: If True, the database column will be set to 'NOT NULL'
        :param datatype: Postgres datatype representation
        :param upsert_strategy: Shows the strategy used when the data for a media item
        is re-ingested: Simple values are replaced with newer non-null values,
        json and array values are merged, some timestamps are set to the execution time.
        :param constraint: Column constraint in database
        :param db_name: Column name in database, if different from TSV name
        """
        self.NAME = name
        self.REQUIRED = required
        self.datatype = datatype
        self.upsert_strategy = upsert_strategy
        self.constraint = constraint
        self.db_name = db_name if db_name is not None else name
        self.nullable = nullable if nullable is not None else not required

    def __str__(self):
        return f"str {type(self).__name__} {self.NAME}"

    def __repr__(self):
        return f"{type(self).__name__} {self.NAME}"

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
                f"Unrecognized column {self.NAME}; setting to NULL during upsert"
            )
            return NULL
        else:
            return strategy(self.db_name)

    def _create_definition(self, is_loading=True):
        dt = self.datatype.value
        constraint = "" if self.constraint is None else f" {self.constraint}"
        if is_loading:
            nullable = ""
        else:
            nullable = "" if self.nullable else " NOT NULL"
        column_string = f"{self.db_name} {dt}{constraint}{nullable}"
        return column_string

    @property
    def column_definition(self):
        return self._create_definition(is_loading=True)

    @property
    def main_column_definition(self):
        return self._create_definition(is_loading=False)


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
        constraint: Optional[str] = None,
        db_name: Optional[str] = None,
    ):
        super().__init__(
            name,
            required,
            datatype=Datatype.int,
            upsert_strategy=UpsertStrategy.newest_non_null,
            constraint=constraint,
            db_name=db_name,
        )

    def prepare_string(self, value):
        """
        Returns a string representation to the best integer approx of input.

        If there is no sane mapping from the input to an integer,
        returns None.

        value: for useful output this should be reasonably castable to an int.
        """
        try:
            number = str(int(float(value)))
        except Exception as e:
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
        upsert_strategy: Optional[UpsertStrategy] = UpsertStrategy.newest_non_null,
        constraint: Optional[str] = None,
        db_name: Optional[str] = None,
    ):
        super().__init__(
            name,
            required,
            datatype=Datatype.bool,
            upsert_strategy=upsert_strategy,
            constraint=constraint,
            db_name=db_name,
        )
        self.constraint = constraint

    def prepare_string(self, value):
        """
        Returns a string `t` or `f`, as appropriate to input.

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
        db_name: Optional[str] = None,
        upsert_strategy: Optional[UpsertStrategy] = None,
    ):
        json_upsert_strategy = (
            UpsertStrategy.merge_jsonb_objects
            if upsert_strategy is None
            else upsert_strategy
        )
        super().__init__(
            name,
            required,
            datatype=Datatype.jsonb,
            upsert_strategy=json_upsert_strategy,
            db_name=db_name,
            constraint=None,
        )

    def prepare_string(self, value):
        """
        Returns a json string as appropriate to input.

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
        Recursively sanitizes the non-dict, non-list values of an input
        dictionary or list in preparation for dumping to JSON string.
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
        size: int,
        truncate: bool,
        db_name: Optional[str] = None,
    ):
        self.SIZE = size
        self.TRUNCATE = truncate
        super().__init__(
            name,
            required,
            datatype=Datatype.char,
            upsert_strategy=UpsertStrategy.newest_non_null,
            constraint=f"varying({size})",
            db_name=db_name,
        )

    def prepare_string(self, value):
        """
        Sanitizes input and enforces the character limit, returning a string.
        """
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
        upsert_strategy: Optional[UpsertStrategy] = None,
    ):
        super().__init__(
            name,
            required=required,
            datatype=Datatype.timestamp,
            upsert_strategy=UpsertStrategy.now
            if not upsert_strategy
            else upsert_strategy,
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
        size: int,
        nullable: bool = False,
        db_name: Optional[str] = None,
    ):
        self.SIZE = size
        super().__init__(
            name,
            required,
            datatype=Datatype.char,
            upsert_strategy=UpsertStrategy.newest_non_null,
            constraint=f"varying({size})",
            db_name=db_name,
            nullable=nullable,
        )

    def prepare_string(self, value):
        """
        Returns input unchanged, as long as it is a valid URL string.

        Also enforces the character limit of the column. If the input
        value fails a validation, returns None.
        """
        if self._Column__sanitize_string(value) != value:
            return None
        else:
            return self._Column__enforce_char_limit(
                urls.validate_url_string(value), self.SIZE, False
            )


class ArrayColumn(Column):
    """
    Represents a PostgreSQL column of type Array, which should hold elements
    of the given BASE_COLUMN type.

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
        base_column: Column,
        db_name: Optional[str] = None,
    ):
        self.BASE_COLUMN = base_column
        super().__init__(
            name,
            required,
            datatype=Datatype.char,
            upsert_strategy=UpsertStrategy.merge_array,
            constraint="varying(80)[]",
            db_name=db_name,
        )

    def prepare_string(self, value):
        """
        Returns a string representation of an array in the PostgreSQL format:
        `{<item 1>, <item 2>...}`.

        Apply changes and validations of the corresponding base column type.
        """
        input_type = type(value)

        if value is None:
            return value
        elif input_type != list:
            arr_str = self.BASE_COLUMN.prepare_string(value)
            return "{" + arr_str + "}" if arr_str else None

        values = []
        for val in value:
            if val is None:
                values.append(None)
            else:
                values.append(self.BASE_COLUMN.prepare_string(val))
        arr_str = json.dumps(values, ensure_ascii=False)
        return "{" + arr_str[1:-1] + "}" if arr_str else None


FOREIGN_ID_COLUMN = StringColumn(
    name="foreign_identifier",
    required=True,
    size=3000,
    truncate=False,
)
LANDING_URL_COLUMN = URLColumn(
    name="foreign_landing_url", required=True, size=1000, nullable=True
)
DIRECT_URL_COLUMN = URLColumn(
    # `url` in DB
    name="url",
    required=True,
    size=3000,
    db_name="url",
)
THUMBNAIL_COLUMN = URLColumn(
    # `thumbnail` in DB
    name="thumbnail_url",
    required=False,
    size=3000,
    db_name="thumbnail",
)
FILESIZE_COLUMN = IntegerColumn(name="filesize", required=False)
LICENSE_COLUMN = StringColumn(
    name="license_", required=True, size=50, truncate=False, db_name="license"
)
LICENSE_VERSION_COLUMN = StringColumn(
    name="license_version", required=True, size=25, truncate=False
)
CREATOR_COLUMN = StringColumn(name="creator", required=False, size=2000, truncate=True)
CREATOR_URL_COLUMN = URLColumn(name="creator_url", required=False, size=2000)
TITLE_COLUMN = StringColumn(name="title", required=False, size=5000, truncate=True)
META_DATA_COLUMN = JSONColumn(name="meta_data", required=False)
TAGS_COLUMN = JSONColumn(
    name="tags", required=False, upsert_strategy=UpsertStrategy.merge_jsonb_arrays
)
WATERMARKED_COLUMN = BooleanColumn(name="watermarked", required=False)
PROVIDER_COLUMN = StringColumn(name="provider", required=False, size=80, truncate=False)
SOURCE_COLUMN = StringColumn(name="source", required=False, size=80, truncate=False)
INGESTION_TYPE_COLUMN = StringColumn(
    name="ingestion_type", required=False, size=80, truncate=False
)
WIDTH_COLUMN = IntegerColumn(name="width", required=False)
HEIGHT_COLUMN = IntegerColumn(name="height", required=False)

DURATION_COLUMN = IntegerColumn(name="duration", required=False)
BIT_RATE_COLUMN = IntegerColumn(
    name="bit_rate",
    required=False,
)

SAMPLE_RATE_COLUMN = IntegerColumn(
    name="sample_rate",
    required=False,
)
CATEGORY_COLUMN = StringColumn(
    name="category",
    required=False,
    size=80,
    truncate=False,
)
GENRES_COLUMN = ArrayColumn(
    name="genres",
    required=False,
    base_column=StringColumn(name="genre", required=False, size=80, truncate=False),
)
AUDIO_SET_COLUMN = JSONColumn(
    # set name, set thumbnail, position of audio in set, set url
    name="audio_set",
    required=False,
)
ALT_FILES_COLUMN = JSONColumn(
    # Alternative files: url, filesize, bit_rate, sample_rate
    name="alt_files",
    required=False,
)

IDENTIFIER_COLUMN = UUIDColumn(
    name="identifier",
)

CREATED_ON_COLUMN = TimestampColumn(
    name="created_on", required=True, upsert_strategy=UpsertStrategy.no_change
)

UPDATED_ON_COLUMN = TimestampColumn(
    name="updated_on",
    required=True,
)

LAST_SYNCED_COLUMN = TimestampColumn(name="last_synced_with_source", required=False)

REMOVED_COLUMN = BooleanColumn(
    name="removed_from_source", required=True, upsert_strategy=UpsertStrategy.false
)

FILETYPE_COLUMN = StringColumn(name="filetype", required=False, truncate=False, size=5)

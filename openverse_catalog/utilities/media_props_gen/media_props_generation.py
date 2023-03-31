"""Automatic media properties generation."""
import ast
import logging
import re
from pathlib import Path
from typing import NamedTuple


log = logging.getLogger(__name__)
# Silence noisy modules
logging.getLogger("common.storage.media").setLevel(logging.WARNING)

# Constants
DOC_MD_PATH = Path(__file__).parent / "media_properties.md"
LOCAL_POSTGRES_FOLDER = Path(__file__).parents[3] / "docker" / "local_postgres"

IMAGE_SQL_PATH = LOCAL_POSTGRES_FOLDER / "0003_openledger_image_schema.sql"
AUDIO_SQL_PATH = LOCAL_POSTGRES_FOLDER / "0006_openledger_audio_schema.sql"

COLUMNS_PATH = Path(__file__).parents[2] / "dags" / "common" / "storage" / "columns.py"


class FieldInfo(NamedTuple):
    name: str
    nullable: bool
    type_: str
    constraint: str
    python_column: str = ""


sql_type_regex = re.compile(
    "(timestamp with time zone|character varying|integer|boolean|jsonb|uuid)"
)


def extract_field_info(fields: list[str], python_columns) -> list[FieldInfo]:
    sql_fields = []
    for field in fields:
        field_name = field.split(" ")[0]
        nullable = False if "not null" in field.lower() else True
        field_constraint = ""
        try:
            field_type = sql_type_regex.search(field).group(1)
            if field_type == "character varying":
                char_limit = field.split("(")[1].split(")")[0]
                field_constraint = f"({char_limit})"
        except AttributeError:
            raise ValueError(f"Could not find type for field {field_name} in {field}")
        py_col = python_columns.get(field_name)
        if not py_col:
            raise ValueError(f"Could not find python column for {field_name}")
        field_info = FieldInfo(
            field_name,
            nullable,
            field_type,
            field_constraint,
            python_column=f"{py_col})",
        )
        sql_fields.append(field_info)
    return sql_fields


def get_media_props_info(media_type, python_columns: dict = None) -> list[FieldInfo]:

    create_table_regex = re.compile(r"CREATE\s+TABLE\s+\w+\.(\w+)\s+\(([\s\S]*?)\);")
    sql_path = {"image": IMAGE_SQL_PATH, "audio": AUDIO_SQL_PATH}[media_type]

    with open(sql_path) as f:
        contents = f.read()
        table_description_matches = create_table_regex.search(contents)
    if not table_description_matches:
        return []
    table_name = table_description_matches.group(1)
    if table_name != media_type:
        print(f"Table name {table_name} does not match media type {media_type}")
        return []
    fields = [
        field.strip()
        for field in table_description_matches.group(2).split("\n")
        if field.strip()
    ]

    return extract_field_info(fields, python_columns or {})


def parse_column_definition(item: ast.Assign) -> dict[str, any] | None:
    column = {
        "python_type": None,
        "name": None,
        "db_name": None,
        "nullable": None,
        "required": False,
        "upsert_strategy": "newest_non_null",
        "custom_column_props": {},
        "description": "",
    }
    if hasattr(item.value, "func") and hasattr(item.value.func, "id"):
        column["python_type"] = item.value.func.id

    if hasattr(item.value, "keywords"):
        for kw in item.value.keywords:
            if hasattr(kw.value, "value"):
                if kw.arg not in column.keys():
                    column["custom_column_props"][kw.arg] = kw.value.value
                else:
                    # upsert_strategy is a special case
                    if hasattr(kw.value, "attr"):
                        column[kw.arg] = kw.value.attr
                    else:
                        column[kw.arg] = kw.value.value
            else:
                if not hasattr(kw.value, "keywords"):
                    continue
                # An Array column that has a base_column
                column_params = ", ".join(
                    [f"{kw2.arg}={kw2.value.value}" for kw2 in kw.value.keywords]
                )
                column["custom_column_props"][
                    kw.arg
                ] = f"{kw.value.func.id}({column_params})"
        if column["db_name"] is None:
            column["db_name"] = column["name"]
        if column["name"] is None:
            return None
        if column["custom_column_props"] == {}:
            del column["custom_column_props"]
        if column["nullable"] is None:
            column["nullable"] = (
                not column["required"] if column["required"] is not None else True
            )
        return column
    return None


def _get_python_column_types() -> dict[str, int]:
    """Get the Python column names and their line numbers from the columns.py file."""
    with open(COLUMNS_PATH) as f:
        file_contents = f.read()
    code = ast.parse(file_contents)
    column_types = {}

    for item in ast.iter_child_nodes(code):
        if isinstance(item, ast.ClassDef):
            if item.name.endswith("Column") and item.name != "Column":
                column_types[item.name] = item.lineno
    return column_types


def _get_python_props() -> dict[str, any]:
    """Get the Python column definitions from the columns.py file."""
    columns = {}

    with open(COLUMNS_PATH) as f:
        contents = f.read()
    code = ast.parse(contents)

    for item in ast.iter_child_nodes(code):
        if isinstance(item, ast.Assign):
            column = parse_column_definition(item)
            if not column:
                continue
            db_name = column["db_name"]
            del column["db_name"]
            columns[db_name] = format_python_column(db_name, column)

    return columns


def generate_media_props_doc() -> str:
    """
    Generate the tables with media properties database column and
    Python objects characteristics.
    """
    python_columns = _get_python_props()

    media_props_doc = f"""# Media Properties
This is a list of the properties of the database columns and Python objects that are used to store and retrieve media data.
It is auto-generated from the source code in utilities/media_props_gen/media_props_generation.py.

## Image Properties\n
{generate_media_props_table(python_columns, media_type="image")}
"""  # noqa 501
    media_props_doc += f"""## Audio Properties\n
{generate_media_props_table(python_columns, media_type="audio")}
"""
    return media_props_doc


def format_python_column(column_db_name: str, python_column: dict[str, any]):
    col_type = python_column.pop("python_type")
    python_column_string = f"{col_type}("
    col_name = python_column.pop("name")
    if col_name != column_db_name:
        python_column_string += f"name='{col_name}', "
    custom_props = python_column.pop("custom_column_props", None)
    custom_props_string = ""
    if custom_props:
        props_string = ", ".join([f"{k}={v}" for k, v in custom_props.items()])
        custom_props_string = f", {col_type}Props({props_string})"
    python_column_string += ", ".join([f"{k}={v}" for k, v in python_column.items()])
    python_column_string += f"{custom_props_string})"

    return python_column_string


def generate_media_props_table(python_columns, media_type="image") -> str:
    """Generate the table with media properties."""

    media_fields = get_media_props_info(media_type, python_columns)

    # Convert the list of FieldInfo objects to a md table
    table = "| DB Field | DB Nullable | DB Type | Python Column |\n"
    table += "| --- | --- | --- | --- |\n"
    for field in media_fields:
        field_db_type = (
            field.type_ if not field.constraint else f"{field.type_} {field.constraint}"
        )
        table += (
            f"| {field.name} | {field.nullable} | "
            f"{field_db_type} | {field.python_column}\n"
        )
        print(field.name)

    return table


def write_media_props_doc(path: Path = DOC_MD_PATH) -> None:
    """Generate the DAG documentation and write it to a file."""
    doc_text = generate_media_props_doc()
    log.info(f"Writing DAG doc to {path}")
    path.write_text(doc_text)


if __name__ == "__main__":
    write_media_props_doc()

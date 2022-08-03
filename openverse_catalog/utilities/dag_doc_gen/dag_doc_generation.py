"""
Notes
- Pair with provider workflows in order to get media type and any other info
- Display media type in provider dag section
- Link to DAG documentation using ## or something so it plays nice with github
- symlink to top level file
"""
import logging
from pathlib import Path
from typing import NamedTuple

import click
from airflow.models import DAG, DagBag


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.INFO
)
log = logging.getLogger(__name__)

# Constants
DAG_MD_PATH = Path(__file__).parent / "DAGs.md"
DAG_FOLDER = Path(__file__).parents[2] / "dags"

# Typing
DagMapping = dict[str, DAG]


class DagInfo(NamedTuple):
    schedule_interval: str | None
    doc: str | None
    dated: bool
    tags: list[str]
    type_: str


def load_dags(dag_folder: str) -> DagMapping:
    dag_bag = DagBag(dag_folder=dag_folder, include_examples=False)
    if dag_bag.import_errors:
        raise ValueError(
            "DagBag could not load properly due to errors with the following DAGs: "
            f"{set(dag_bag.import_errors.keys())}"
        )
    return dag_bag.dags


def get_dag_info(dags: DagMapping) -> dict[str, DagInfo]:
    dags_info = {}
    for dag_id, dag in dags.items():
        doc = dag.doc_md or "_No documentation provided._"
        dated = dag.catchup
        tags = dag.tags or []
        # Infer dag type from the first available tag
        type_ = tags[0] if tags else "Other"
        dags_info[dag_id] = DagInfo(
            schedule_interval=dag.schedule_interval,
            doc=doc,
            dated=dated,
            tags=tags,
            type_=type_,
        )

    return dags_info


def generate_dag_doc() -> str:
    text = """\
# DAGs

_Note: this document is auto-generated and should not be manually edited_

This document describes the DAGs available along with pertinent DAG information and
the DAG's documentation.

## Available DAGs
"""

    dags = load_dags(str(DAG_FOLDER))
    dags_info = get_dag_info(dags)

    for dag_id, dag_info in dags_info.items():
        text += (
            f" - `{dag_id}`: {dag_info.schedule_interval}, dated={dag_info.dated}"
            f", tags={dag_info.tags}, type={dag_info.type_}\n"
        )

    return text


def write_dag_doc() -> None:
    doc_text = generate_dag_doc()
    log.info(f"Writing DAG doc to {DAG_MD_PATH}")
    DAG_MD_PATH.write_text(doc_text)


@click.command()
def cli():
    write_dag_doc()


if __name__ == "__main__":
    cli()

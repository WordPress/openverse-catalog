"""
Notes
- Pair with provider workflows in order to get media type and any other info
- Display media type in provider dag section
- Link to DAG documentation using ## or something so it plays nice with github
- symlink to top level file
"""
import logging
from collections import defaultdict
from pathlib import Path
from typing import NamedTuple

import click
from airflow.models import DAG, DagBag
from providers.provider_workflows import PROVIDER_WORKFLOWS, ProviderWorkflow


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
    dag_id: str
    schedule: str | None
    doc: str | None
    type_: str
    dated: bool
    provider_workflow: ProviderWorkflow | None


def load_dags(dag_folder: str) -> DagMapping:
    dag_bag = DagBag(dag_folder=dag_folder, include_examples=False)
    if dag_bag.import_errors:
        raise ValueError(
            "DagBag could not load properly due to errors with the following DAGs: "
            f"{set(dag_bag.import_errors.keys())}"
        )
    return dag_bag.dags


def get_provider_workflows() -> dict[str, ProviderWorkflow]:
    return {workflow.dag_id: workflow for workflow in PROVIDER_WORKFLOWS}


def get_dag_info(dags: DagMapping) -> list[DagInfo]:
    dags_info = []
    provider_workflows = get_provider_workflows()
    for dag_id, dag in dags.items():
        doc = dag.doc_md or "_No documentation provided._"
        dated = dag.catchup
        # Infer dag type from the first available tag
        type_ = dag.tags[0] if dag.tags else "other"
        dags_info.append(
            DagInfo(
                dag_id=dag_id,
                schedule=dag.schedule_interval,
                doc=doc,
                type_=type_,
                dated=dated,
                provider_workflow=provider_workflows.get(dag_id),
            )
        )

    return dags_info


def generate_section(type_: str, dags_info: list[DagInfo]) -> str:
    log.info(f"Building section for '{type_}'")
    name = type_.replace("_", " ").replace("-", " ").title()
    text = f"## {name}\n\n"
    header = "| DAG ID | Schedule Interval |"
    is_provider = type_ == "provider"
    if is_provider:
        header += " Dated | Media Type(s) |"

    column_count = len(header.split("|")) - 2
    log.info(f"Total columns: {column_count + 1}")
    text += header + "\n"
    text += "| " + " | ".join(["---"] * column_count) + " |"

    for dag in dags_info:
        text += f"\n| `{dag.dag_id}` | `{dag.schedule}` |"
        if is_provider:
            text += f" `{dag.dated}` | {', '.join(dag.provider_workflow.media_types)} |"

    text += "\n\n"

    return text


def generate_dag_doc(dag_folder: Path = DAG_FOLDER) -> str:
    text = """\
# DAGs

_Note: this document is auto-generated and should not be manually edited_

This document describes the DAGs available along with pertinent DAG information and
the DAG's documentation.

# Available DAGs
"""

    dags = load_dags(str(dag_folder))
    dags_info = get_dag_info(dags)

    dags_by_section: dict[str, list[DagInfo]] = defaultdict(list)
    for dag in dags_info:
        dags_by_section[dag.type_].append(dag)

    for section, dags in sorted(dags_by_section.items()):
        text += generate_section(section, dags)

    return text


def write_dag_doc(path: Path = DAG_MD_PATH) -> None:
    doc_text = generate_dag_doc()
    log.info(f"Writing DAG doc to {path}")
    path.write_text(doc_text)


@click.command()
def cli():
    write_dag_doc()


if __name__ == "__main__":
    cli()

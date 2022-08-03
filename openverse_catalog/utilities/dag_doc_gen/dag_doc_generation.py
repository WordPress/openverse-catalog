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


log = logging.getLogger(__name__)
# Silence noisy modules
logging.getLogger("common.storage.media").setLevel(logging.WARNING)

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
        doc = dag.doc_md
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


def generate_section(name: str, dags_info: list[DagInfo], is_provider: bool) -> str:
    log.info(f"Building section for '{name}'")
    text = f"## {name}\n\n"
    header = "| DAG ID | Schedule Interval |"
    if is_provider:
        header += " Dated | Media Type(s) |"

    column_count = len(header.split("|")) - 2
    log.info(f"Total columns: {column_count + 1}")
    text += header + "\n"
    text += "| " + " | ".join(["---"] * column_count) + " |"

    for dag in dags_info:
        dag_id = f"`{dag.dag_id}`"
        if dag.doc:
            dag_id = f"[{dag_id}](#{dag.dag_id})"
        text += f"\n| {dag_id} | `{dag.schedule}` |"
        if is_provider:
            text += f" `{dag.dated}` | {', '.join(dag.provider_workflow.media_types)} |"

    text += "\n\n"

    return text


def generate_documentation(dag: DagInfo) -> str:
    return f"""
## `{dag.dag_id}`

{dag.doc}

"""


def generate_dag_doc(dag_folder: Path = DAG_FOLDER) -> str:
    text = """\
# DAGs

_Note: this document is auto-generated and should not be manually edited_

This document describes the DAGs available along with pertinent DAG information and
the DAG's documentation.

# DAGs by Type

The following are DAGs grouped by their primary tag.

"""

    dags = load_dags(str(dag_folder))
    dags_info = get_dag_info(dags)

    dag_sections = []

    dags_by_section: dict[str, list[DagInfo]] = defaultdict(list)
    for dag in dags_info:
        dags_by_section[dag.type_].append(dag)

    for section, dags in sorted(dags_by_section.items()):
        name = section.replace("_", " ").replace("-", " ").title()
        is_provider = section == "provider"
        text += f" 1. [{name}](#{section})\n"
        dag_sections.append(generate_section(name, dags, is_provider))

    text += "\n" + "\n\n".join(dag_sections)

    text += """
# DAG documentation

The following is documentation associated with each DAG (where available)

"""
    dag_docs = []
    for dag in sorted(dags_info, key=lambda d: d.dag_id):
        if not dag.doc:
            continue
        text += f" 1. [`{dag.dag_id}`](#{dag.dag_id})\n"
        dag_docs.append(generate_documentation(dag))

    text += "\n" + "".join(dag_docs)

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

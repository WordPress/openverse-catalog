"""
Automatic DAG documentation generator.

This script generates a markdown documentation file which aggregates various pieces
of information about all of our DAGs. The generated document has two sections: "DAGs
by type" and "individual DAG documentation". Both sections have a small table of
contents.

The DAGs-by-type section shows DAG ID and schedule interval for all DAGS, and also shows
dated & media type info for provider DAGs. Where possible, the DAG IDs link to
individual documentation subsections further in the document.

The individual DAG documentation section pulls the DAG's `doc_md` blurb and renders
it within the document.
"""
import logging
from collections import defaultdict
from pathlib import Path
from typing import NamedTuple

from airflow.models import DAG, DagBag

from providers.provider_workflows import PROVIDER_WORKFLOWS, ProviderWorkflow


log = logging.getLogger(__name__)
# Silence noisy modules
logging.getLogger("common.storage.media").setLevel(logging.WARNING)

# Constants
DAG_MD_PATH = Path(__file__).parent / "DAGs.md"
DAG_FOLDER = Path(__file__).parents[2] / "dags"
PREAMBLE = """\
# DAGs

_Note: this document is auto-generated and should not be manually edited_

This document describes the DAGs available along with pertinent DAG information and
the DAG's documentation.

The DAGs are shown in two forms:

 - [DAGs by Type](#dags-by-type)
 - [Individual DAG documentation](#dag-documentation)

# DAGs by Type

The following are DAGs grouped by their primary tag:

"""
MIDAMBLE = """
# DAG documentation

The following is documentation associated with each DAG (where available):

"""

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
    """
    Load and return the DAGs in the provided dag folder. Execution will stop if any DAGs
    could not be imported.
    """
    dag_bag = DagBag(dag_folder=dag_folder, include_examples=False)
    if dag_bag.import_errors:
        raise ValueError(
            "DagBag could not load properly due to errors with the following DAGs: "
            f"{set(dag_bag.import_errors.keys())}"
        )
    return dag_bag.dags


def get_provider_workflows() -> dict[str, ProviderWorkflow]:
    """
    Extract the provider workflows from the PROVIDER_WORKFLOWS list and create a mapping
    from DAG ID to workflow.
    """
    return {workflow.dag_id: workflow for workflow in PROVIDER_WORKFLOWS}


def get_dags_info(dags: DagMapping) -> list[DagInfo]:
    """
    Convert the provided DAG ID -> DAG mapping into a list of DagInfo instances.
    Provider information is added where available.
    """
    dags_info = []
    provider_workflows = get_provider_workflows()
    for dag_id, dag in dags.items():
        doc = dag.doc_md
        # Convert the initial H1 header level to H3 if it exists, for better formatting
        # within the document
        if doc and doc.strip().startswith("# "):
            doc = "### " + doc.strip()[2:]
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


def generate_type_subsection(
    name: str, dags_info: list[DagInfo], is_provider: bool
) -> str:
    """Generate the documentation for a "DAGs by type" subsection."""
    log.info(f"Building subsection for '{name}'")
    text = f"## {name}\n\n"
    # Columns for all DAGs
    header = "| DAG ID | Schedule Interval |"
    # Conditionally add the other columns for the provider-specific DAGs
    if is_provider:
        header += " Dated | Media Type(s) |"

    # In order to create a table of the appropriate width, we need the number of columns
    # to complete the second row of the header (the "---" columns). There are
    # columns + 1 pipe characters, so stripping the pipes on both ends then splitting
    # gives us the appropriate number of columns.
    column_count = len(header.strip("|").split("|"))
    log.info(f"Total columns: {column_count}")
    text += header + "\n"
    text += "| " + " | ".join(["---"] * column_count) + " |"

    for dag in dags_info:
        dag_id = f"`{dag.dag_id}`"
        # If we have documentation for the DAG, we'll want to link to it within the
        # markdown, so we reference it using the heading text (the DAG ID)
        if dag.doc:
            dag_id = f"[{dag_id}](#{dag.dag_id})"
        text += f"\n| {dag_id} | `{dag.schedule}` |"
        if is_provider:
            text += f" `{dag.dated}` | {', '.join(dag.provider_workflow.media_types)} |"

    text += "\n\n"

    return text


def generate_single_documentation(dag: DagInfo) -> str:
    """Generate the documentation for a single DAG."""
    return f"""
## `{dag.dag_id}`

{dag.doc}

"""


def generate_dag_doc(dag_folder: Path = DAG_FOLDER) -> str:
    """
    Generate the DAG documentation markdown file using the DAGs available in the
    folder provided.
    """
    text = PREAMBLE
    dags = load_dags(str(dag_folder))
    # DAGs come out of the DagBag in seemingly random order, so sort them by DAG ID
    # before any operations are run on them.
    dags_info = sorted(get_dags_info(dags), key=lambda x: x.dag_id)

    dag_types = []

    # Group DAGs them into sub-lists by DAG "type", which is determined by the first
    # available DAG tag.
    dags_by_type: dict[str, list[DagInfo]] = defaultdict(list)
    for dag in dags_info:
        dags_by_type[dag.type_].append(dag)

    for type_, dags in sorted(dags_by_type.items()):
        # Create a more human-readable name
        name = type_.replace("_", " ").replace("-", " ").title()
        # Special case for provider tables since they have extra information
        is_provider = type_ == "provider"
        # For each type we generate a sub-list of DAGs. We add a link to each generated
        # sub-list as part of a table of contents, but defer adding the sub-lists until
        # all are generated.
        text += f" 1. [{name}](#{type_})\n"
        dag_types.append(generate_type_subsection(name, dags, is_provider))

    text += "\n" + "\n\n".join(dag_types)

    text += MIDAMBLE
    dag_docs = []
    for dag in sorted(dags_info, key=lambda d: d.dag_id):
        # This section only contains subsections for DAGs where we have documentation
        if not dag.doc:
            continue
        # Similar to the DAGs-by-type section, we add the reference to a table of
        # contents first, and then defer adding all the generated individual docs until
        # the very end.
        text += f" 1. [`{dag.dag_id}`](#{dag.dag_id})\n"
        dag_docs.append(generate_single_documentation(dag))

    text += "\n" + "".join(dag_docs)

    # Normalize the newlines at the end of the file and add one more to make sure
    # our pre-commit checks are happy!
    return text.strip() + "\n"


def write_dag_doc(path: Path = DAG_MD_PATH) -> None:
    """Generate the DAG documentation and write it to a file."""
    doc_text = generate_dag_doc()
    log.info(f"Writing DAG doc to {path}")
    path.write_text(doc_text)


if __name__ == "__main__":
    write_dag_doc()

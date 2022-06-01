from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Optional, Sequence


@dataclass
class ProviderWorkflow:
    """
    Required Arguments:

    dag_id:           string giving a unique id of the DAG to be created.
    provider_script:  string path for the provider_script file whose main
                      function is to be run. If the optional argument
                      `dated` is True, then the function must take a
                      single parameter (date) which will be a string of
                      the form 'YYYY-MM-DD'. Otherwise, the function
                      should take no arguments.

    Optional Arguments:

    default_args:      dictionary which is passed to the airflow.dag.DAG
                       __init__ method.
    start_date:        datetime.datetime giving the first valid execution
                       date of the DAG.
    max_active_tasks:  integer that sets the number of tasks which can
                       run simultaneously for this DAG, and the number of
                       dagruns of this DAG which can be run in parallel.
                       It's important to keep the rate limits of the
                       Provider API in mind when setting this parameter.
    schedule_string:   string giving the schedule on which the DAG should
                       be run.  Passed to the airflow.dag.DAG __init__
                       method.
    dated:             boolean giving whether the `main_function` takes a
                       string parameter giving a date (i.e., the date for
                       which data should be ingested).
    day_shift:         integer giving the number of days before the
                       current execution date the `main_function` should
                       be run (if `dated=True`).
    execution_timeout: datetime.timedelta giving the amount of time a given data
                       pull may take.
    doc_md:            string which should be used for the DAG's documentation markdown
    media_types:       list describing the media type(s) that this provider handles
                       (e.g. `["audio"]`, `["image", "audio"]`, etc.)
    """

    dag_id: str
    provider_script: str
    default_args: Optional[Dict] = None
    start_date: datetime = datetime(1970, 1, 1)
    max_active_tasks: int = 1
    schedule_string: str = "@monthly"
    dated: bool = False
    day_shift: int = 0
    execution_timeout: timedelta = timedelta(hours=12)
    doc_md: str = ""
    media_types: Sequence[str] = ("image",)


PROVIDER_WORKFLOWS = [
    ProviderWorkflow(
        dag_id="brooklyn_museum_workflow",
        provider_script="brooklyn_museum",
        start_date=datetime(2020, 1, 1),
        execution_timeout=timedelta(hours=24),
    ),
    ProviderWorkflow(
        dag_id="cleveland_museum_workflow",
        provider_script="cleveland_museum_of_art",
        start_date=datetime(2020, 1, 15),
    ),
    ProviderWorkflow(
        dag_id="europeana_workflow",
        provider_script="europeana",
        schedule_string="@daily",
        dated=True,
    ),
    ProviderWorkflow(
        dag_id="finnish_museums_workflow",
        provider_script="finnish_museums",
        start_date=datetime(2020, 9, 1),
        execution_timeout=timedelta(days=3),
    ),
    ProviderWorkflow(
        dag_id="flickr_wokrflow",
        provider_script="flickr",
        schedule_string="@daily",
        dated=True,
    ),
    ProviderWorkflow(
        dag_id="freesound_workflow",
        provider_script="freesound",
        # doc_md = freesound.__doc__,
        media_types=["audio,"],
    ),
    ProviderWorkflow(
        dag_id="jamendo_workflow",
        provider_script="jamendo",
        execution_timeout=timedelta(hours=24),
        media_types=[
            "audio",
        ],
    ),
    ProviderWorkflow(
        dag_id="metropolitan_museum_workflow",
        provider_script="metropolitan_museum_of_art",
        schedule_string="@daily",
        dated=True,
    ),
    ProviderWorkflow(
        dag_id="museum_victoria_workflow",
        provider_script="museum_victoria",
        start_date=datetime(2020, 1, 1),
        execution_timeout=timedelta(hours=24),
    ),
    ProviderWorkflow(
        dag_id="nypl_workflow",
        provider_script="nypl",
        start_date=datetime(2020, 1, 1),
        execution_timeout=timedelta(hours=24),
    ),
    ProviderWorkflow(
        dag_id="phylopic_workflow",
        provider_script="phylopic",
        schedule_string="@weekly",
        dated=True,
    ),
    ProviderWorkflow(
        dag_id="rawpixel_workflow",
        provider_script="raw_pixel",
    ),
    ProviderWorkflow(
        dag_id="science_museum_workflow",
        provider_script="science_museum",
        start_date=datetime(2020, 1, 1),
        execution_timeout=timedelta(hours=24),
    ),
    ProviderWorkflow(
        dag_id="smithsonian_workflow",
        provider_script="smithsonian",
        start_date=datetime(2020, 1, 1),
        schedule_string="@weekly",
        execution_timeout=timedelta(hours=24),
    ),
    ProviderWorkflow(
        dag_id="smk_workflow",
        provider_script="smk",
        start_date=datetime(2020, 1, 1),
        execution_timeout=timedelta(hours=24),
    ),
    ProviderWorkflow(
        dag_id="stocksnap_workflow",
        provider_script="stocksnap",
    ),
    ProviderWorkflow(
        dag_id="walters_workflow",
        provider_script="walters_art_museum",
        start_date=datetime(2020, 9, 27),
        execution_timeout=timedelta(hours=24),
    ),
    ProviderWorkflow(
        dag_id="wikimedia_commons_workflow",
        provider_script="wikimedia_commons",
        schedule_string="@daily",
        dated=True,
        media_types=["image", "audio"],
    ),
    ProviderWorkflow(
        dag_id="wordpress_workflow",
        provider_script="wordpress",
    ),
]

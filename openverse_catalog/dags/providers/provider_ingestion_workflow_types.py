from dataclasses import dataclass
from datetime import datetime, timedelta

from providers.provider_workflow_types import ProviderWorkflow


@dataclass
class ProviderIngestionWorkflow(ProviderWorkflow):
    """
    Extends the ProviderWorkflow with configuration options used to set up
    a day-partitioned ingestion workflow DAG.

    Optional Arguments:

    daily_list_length:       int number of daily DagRuns that should be run
    weekly_list_length:      int number of weekly DagRuns that should be run
    fortnightly_list_length: int number of DagRuns that should be run per
                             fifteen-day interval
    one_month_list_length:   int number of monthly DagRuns that should be run
    three_month_list_length: int number of DagRuns that should be run per
                             three-month interval
    six_month_list_length:   int number of DagRuns that should be run per
                             six-month interval
    dagrun_timeout:          datetime.timedelta giving the amount of allowed
                             time for the total ingestion to take, including all
                             calls to the `main_function`.
    """

    daily_list_length: int = 0
    weekly_list_length: int = 0
    fortnightly_list_length: int = 0
    one_month_list_length: int = 0
    three_month_list_length: int = 0
    six_month_list_length: int = 0
    dagrun_timeout: timedelta = timedelta(hours=23)

    # Override default ProviderWorkflow configurations
    schedule_string: str = "@daily"
    dated = True


PROVIDER_INGESTION_WORKFLOWS = [
    ProviderIngestionWorkflow(
        dag_id="europeana_ingestion_workflow",
        provider_script="europeana",
        start_date=datetime(2013, 11, 21),
        max_active_tasks=3,
        execution_timeout=timedelta(hours=12),
        daily_list_length=7,
        one_month_list_length=12,
        three_month_list_length=40,
    ),
    ProviderIngestionWorkflow(
        dag_id="flickr_ingestion_workflow",
        provider_script="flickr",
        execution_timeout=timedelta(minutes=30),
        daily_list_length=7,
        weekly_list_length=12,
        fortnightly_list_length=20,
        one_month_list_length=24,
        three_month_list_length=24,
        six_month_list_length=40,
    ),
    ProviderIngestionWorkflow(
        dag_id="wikimedia_ingestion_workflow",
        provider_script="wikimedia_commons",
        execution_timeout=timedelta(minutes=90),
        media_types=("image", "audio"),
        max_active_tasks=2,
        daily_list_length=6,
        one_month_list_length=9,
        three_month_list_length=18,
        six_month_list_length=30,
    ),
]

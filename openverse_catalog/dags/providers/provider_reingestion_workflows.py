from dataclasses import dataclass
from datetime import timedelta

from providers.provider_api_scripts.europeana import EuropeanaDataIngester
from providers.provider_api_scripts.flickr import FlickrDataIngester
from providers.provider_api_scripts.metropolitan_museum import MetMuseumDataIngester
from providers.provider_api_scripts.phylopic import PhylopicDataIngester
from providers.provider_api_scripts.wikimedia_commons import (
    WikimediaCommonsDataIngester,
)
from providers.provider_workflows import ProviderWorkflow


@dataclass
class ProviderReingestionWorkflow(ProviderWorkflow):
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
    schedule_string: str = "@weekly"
    dated: bool = True

    def __post_init__(self):
        if not self.dag_id:
            # Call super() first to initialize the provider_name
            super().__post_init__()
            # Override the dag_id
            self.dag_id = f"{self.provider_name}_reingestion_workflow"
            return

        super().__post_init__()


PROVIDER_REINGESTION_WORKFLOWS = [
    ProviderReingestionWorkflow(
        # 60 total reingestion days
        ingestion_callable=EuropeanaDataIngester,
        max_active_tasks=3,
        pull_timeout=timedelta(hours=12),
        daily_list_length=7,
        one_month_list_length=12,
        three_month_list_length=40,
    ),
    ProviderReingestionWorkflow(
        # 128 total reingestion days
        ingestion_callable=FlickrDataIngester,
        pull_timeout=timedelta(minutes=30),
        daily_list_length=7,
        weekly_list_length=12,
        fortnightly_list_length=20,
        one_month_list_length=24,
        three_month_list_length=24,
        six_month_list_length=40,
    ),
    ProviderReingestionWorkflow(
        # 64 total reingestion days
        ingestion_callable=MetMuseumDataIngester,
        max_active_tasks=2,
        pull_timeout=timedelta(hours=12),
        daily_list_length=6,
        one_month_list_length=9,
        three_month_list_length=18,
        six_month_list_length=30,
    ),
    ProviderReingestionWorkflow(
        # 64 total reingestion days
        ingestion_callable=PhylopicDataIngester,
        max_active_tasks=2,
        pull_timeout=timedelta(hours=12),
        daily_list_length=6,
        one_month_list_length=9,
        three_month_list_length=18,
        six_month_list_length=30,
    ),
    ProviderReingestionWorkflow(
        # 64 total reingestion days
        dag_id="wikimedia_reingestion_workflow",
        ingestion_callable=WikimediaCommonsDataIngester,
        pull_timeout=timedelta(minutes=90),
        max_active_tasks=2,
        daily_list_length=6,
        one_month_list_length=9,
        three_month_list_length=18,
        six_month_list_length=30,
    ),
]

"""
# Refresh Popularity Metrics TaskGroup Factory
This file generates a TaskGroup that refreshes the underlying popularity DB
tables, using a factory function.

This step updates any changes to popularity metrics, and recalculates the
popularity constants. It should be run at least once every month, or whenever
a new popularity metric is added. Scheduling is handled in the parent data
refresh DAG.
"""
from airflow.utils.task_group import TaskGroup
from common.constants import POSTGRES_CONN_ID
from common.popularity import operators


GROUP_ID = "refresh_popularity_metrics_and_constants"


def create_refresh_popularity_metrics_task_group(media_type: str):
    """
    This factory method instantiates a TaskGroup that will update the popularity
    DB tables for the given media type, including percentiles and popularity
    metrics.

    Required Arguments:

    media_type:  the type of record to refresh
    """
    with TaskGroup(group_id=GROUP_ID) as refresh_all_popularity_data:
        # Update the popularity metrics table, adding any new popularity metrics
        # and updating the configured percentile.
        update_metrics = operators.update_media_popularity_metrics(
            POSTGRES_CONN_ID,
            media_type=media_type,
        )

        # Update the popularity constants view. This completely recalculates the
        # popularity constant for each provider.
        update_constants = operators.update_media_popularity_constants(
            POSTGRES_CONN_ID,
            media_type=media_type,
        )

        update_metrics >> update_constants

    return refresh_all_popularity_data

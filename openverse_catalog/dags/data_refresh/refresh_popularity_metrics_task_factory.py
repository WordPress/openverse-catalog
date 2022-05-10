"""
# Refresh Popularity Metrics TaskGroup Factory
This file generates a TaskGroup that refreshes the underlying popularity DB
tables, using a factory function.

This step updates any changes to popularity metrics, and recalculates the
popularity constants. It should be run at least once every month, or whenever
a new popularity metric is added. Scheduling is handled in the parent data
refresh DAG.
"""
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from common.constants import POSTGRES_CONN_ID, MediaType
from common.popularity import sql


GROUP_ID = "refresh_popularity_metrics_and_constants"
UPDATE_MEDIA_POPULARITY_METRICS_TASK_ID = "update_media_popularity_metrics_table"
UPDATE_MEDIA_POPULARITY_CONSTANTS_TASK_ID = "update_media_popularity_constants_view"


def create_refresh_popularity_metrics_task_group(media_type: MediaType):
    """
    This factory method instantiates a TaskGroup that will update the popularity
    DB tables for the given media type, including percentiles and popularity
    metrics.

    Required Arguments:

    media_type:  the type of record to refresh
    """
    with TaskGroup(group_id=GROUP_ID) as refresh_all_popularity_data:
        update_metrics = PythonOperator(
            task_id=UPDATE_MEDIA_POPULARITY_METRICS_TASK_ID,
            python_callable=sql.update_media_popularity_metrics,
            op_args=[POSTGRES_CONN_ID, media_type],
            doc=(
                "Updates the popularity metrics table, adding any new "
                "popularity metrics and updating the configured percentile."
            ),
        )

        update_constants = PythonOperator(
            task_id=UPDATE_MEDIA_POPULARITY_CONSTANTS_TASK_ID,
            python_callable=sql.update_media_popularity_constants,
            op_args=[POSTGRES_CONN_ID, media_type],
            doc=(
                "Updates the popularity constants view. This completely "
                "recalculates the popularity constants for each provider."
            ),
        )

        update_metrics >> update_constants

    return refresh_all_popularity_data

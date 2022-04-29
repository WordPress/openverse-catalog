"""
# Refresh Materialized View Task Factory
This file generates a Task that refreshes the materialized view for a
given media type, using a factory function.

The task refreshes the materialized view, but not the underlying tables. This
means that the only effect is to add or update data (including popularity data)
for records which have been ingested since the last time the view was
refreshed.

This should be run every time before a data refresh is triggered.
"""
import os

from airflow.utils.trigger_rule import TriggerRule
from common.popularity import operators


DB_CONN_ID = os.getenv("OPENLEDGER_CONN_ID", "postgres_openledger_testing")


def create_refresh_view_data_task(media_type: str):
    """
    This factory method generates a task that will refresh the materialized view for
    the given media type. The view collates popularity data for each record. Refreshing
    it will add and update data for any records that were ingested since the last
    view refresh.

    Required Arguments:

    media_type: the type of record to refresh
    """
    refresh_matview = operators.update_db_view(DB_CONN_ID, media_type=media_type)

    # The preceding refresh_popularity_metrics task is conditional and may be skipped.
    # The matview refresh should run regardless, as long as no upstream task failed.
    refresh_matview.trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS

    return refresh_matview

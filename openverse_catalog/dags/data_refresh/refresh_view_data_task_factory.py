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
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from common.constants import POSTGRES_CONN_ID
from common.popularity import sql


UPDATE_DB_VIEW_TASK_ID = "update_materialized_popularity_view"


def create_refresh_view_data_task(media_type: str):
    """
    The task refreshes the materialized view for the given media type. The view collates
    popularity data for each record. Refreshing has the effect of adding popularity data
    for records that were ingested since the last time the view was refreshed, and
    updating popularity data for existing records.

    Required Arguments:

    media_type: the type of record to refresh
    """
    refresh_matview = PythonOperator(
        task_id=UPDATE_DB_VIEW_TASK_ID,
        python_callable=sql.update_db_view,
        op_args=[POSTGRES_CONN_ID, media_type],
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        doc_md=create_refresh_view_data_task.__doc__,
    )

    return refresh_matview

"""
Manages weekly database snapshots. RDS does not support weekly snapshots
schedules on its own, so we need a DAG to manage this for us.

It runs on Saturdays at 00:00 UTC in order to happen before the data refresh.

The DAG will automatically delete the oldest snapshots when more snaphots
exist than it is configured to retain.

Requires two variables:

`AIRFLOW_RDS_ARN`: The ARN of the RDS DB instance that needs snapshots.
`AIRFLOW_RDS_SNAPSHOTS_TO_RETAIN`: How many historical snapshots to retain.
"""

import logging
from datetime import datetime

import boto3
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.rds import RdsCreateDbSnapshotOperator
from airflow.providers.amazon.aws.sensors.rds import RdsSnapshotExistenceSensor


logger = logging.getLogger(__name__)

DAG_ID = "check_silenced_dags"
MAX_ACTIVE = 1

AIRFLOW_RDS_ARN = Variable.get("AIRFLOW_RDS_ARN")
AIRFLOW_RDS_SNAPSHOTS_TO_RETAIN = Variable.get(
    "AIRFLOW_RDS_SNAPSHOTS_TO_RETAIN", default_var="7"
)


@task()
def delete_previous_snapshots():
    rds = boto3.client("rds")
    # Snapshot object documentation:
    # https://docs.aws.amazon.com/AmazonRDS/latest/APIReference/API_DBSnapshot.html
    snapshots = rds.describe_db_snapshots(
        DBInstanceIdentifier=AIRFLOW_RDS_ARN,
    )

    to_retain = int(AIRFLOW_RDS_SNAPSHOTS_TO_RETAIN)

    if len(snapshots) <= to_retain or not (
        snapshots_to_delete := snapshots[to_retain:]
    ):
        logger.info("No snapshots to delete.")
        return

    logger.info(f"Deleting {len(snapshots_to_delete)} snapshots.")
    for snapshot in snapshots_to_delete:
        logger.info(f"Deleting {snapshot['DBSnapshotIdentifier']}.")
        rds.delete_db_snapshot(DBSnapshotIdentifier=snapshot["DBSnapshotIdentifier"])


@dag(
    dag_id=DAG_ID,
    # At 00:00 on Saturday, this puts it before the data refresh starts
    schedule="0 0 * * 6",
    start_date=datetime(2022, 12, 2),
    tags=["maintenance"],
    max_active_tasks=MAX_ACTIVE,
    max_active_runs=MAX_ACTIVE,
    catchup=False,
    # Use the docstring at the top of the file as md docs in the UI
    doc_md=__doc__,
)
def rotate_db_snapshots():
    snapshot_id = "airflow-{{ ds }}"
    create_db_snapshot = RdsCreateDbSnapshotOperator(
        task_id="create_snapshot",
        db_type="instance",
        db_identifier=AIRFLOW_RDS_ARN,
        db_snapshot_identifier=snapshot_id,
    )
    wait_for_snapshot_availability = RdsSnapshotExistenceSensor(
        task_id="await_snapshot_availability",
        db_type="instance",
        db_snapshot_identifier=snapshot_id,
        # This is the default for ``target_statuses`` but making it explicit is clearer
        target_statuses=["available"],
    )

    create_db_snapshot >> wait_for_snapshot_availability >> delete_previous_snapshots()


rotate_db_snapshots()

import logging

import boto3
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.rds import RdsCreateDbSnapshotOperator
from airflow.providers.amazon.aws.sensors.rds import RdsSnapshotExistenceSensor


logger = logging.getLogger(__name__)


AIRFLOW_RDS_ARN = Variable.get("AIRFLOW_RDS_ARN")
AIRFLOW_RDS_SNAPSHOTS_TO_RETAIN = Variable.get(
    "AIRFLOW_RDS_SNAPSHOTS_TO_RETAIN", default_var="7"
)


@dag(
    # At 00:00 on Saturday, this puts it before the data refresh starts
    schedule="0 0 * * 6"
)
def rotate_db_snapshots():
    snapshot_id = "airflow-{{ ds }}"
    create_db_snapshot = RdsCreateDbSnapshotOperator(
        db_type="instance",
        db_identifier=AIRFLOW_RDS_ARN,
        db_snapshot_identifier=snapshot_id,
    )
    wait_for_snapshot_availability = RdsSnapshotExistenceSensor(
        db_type="instance",
        db_snapshot_identifier=snapshot_id,
        # This is the default for ``target_statuses`` but making it explicit is clearer
        target_statuses=["available"],
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
            logger.log("No snapshots to delete.")
            return

        logger.log(f"Deleting {len(snapshots_to_delete)} snapshots.")
        for snapshot in snapshots_to_delete:
            logger.log(f"Deleting {snapshot['DBSnapshotIdentifier']}.")
            rds.delete_db_snapshot(
                DBSnapshotIdentifier=snapshot["DBSnapshotIdentifier"]
            )

    create_db_snapshot >> wait_for_snapshot_availability >> delete_previous_snapshots()


rotate_db_snapshots()

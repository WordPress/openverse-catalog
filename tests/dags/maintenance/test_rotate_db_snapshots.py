from unittest import mock

import boto3
import pytest
from maintenance.rotate_db_snapshots import delete_previous_snapshots


@pytest.fixture
def rds_client(monkeypatch):
    rds = mock.MagicMock()

    def get_client(*args, **kwargs):
        return rds

    monkeypatch.setattr(boto3, "client", get_client)
    return rds


def _make_snapshot(_id):
    return {"DBSnapshotIdentifier": _id}


@pytest.mark.parametrize(
    "snapshots",
    (
        # Less than 7
        [_make_snapshot(1)],
        # Exactly the number we want to keep
        [_make_snapshot(i) for i in range(7)],
    ),
)
def test_delete_previous_snapshots_no_snapshots_to_delete(snapshots, rds_client):
    rds_client.describe_db_snapshots.return_value = snapshots
    delete_previous_snapshots.function()
    rds_client.delete_db_snapshot.assert_not_called()


def test_delete_previous_snapshots(rds_client):
    snapshots = [_make_snapshot(i) for i in range(10)]
    snapshots_to_delete = snapshots[7:]
    rds_client.describe_db_snapshots.return_value = snapshots
    delete_previous_snapshots.function()
    rds_client.delete_db_snapshot.assert_has_calls(
        [
            mock.call(DBSnapshotIdentifier=snapshot["DBSnapshotIdentifier"])
            for snapshot in snapshots_to_delete
        ]
    )

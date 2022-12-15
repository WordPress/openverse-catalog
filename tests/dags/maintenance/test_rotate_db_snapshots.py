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
    ("snapshots", "snapshots_to_retain"),
    (
        # Less than 7
        ([_make_snapshot(1)], 2),
        ([_make_snapshot(1)], 5),
        # Exactly the number we want to keep
        ([_make_snapshot(i) for i in range(7)], 7),
        ([_make_snapshot(i) for i in range(2)], 2),
    ),
)
def test_delete_previous_snapshots_no_snapshots_to_delete(
    snapshots, snapshots_to_retain, rds_client
):
    rds_client.describe_db_snapshots.return_value = snapshots
    delete_previous_snapshots.function("fake_arn", snapshots_to_retain)
    rds_client.delete_db_snapshot.assert_not_called()


def test_delete_previous_snapshots(rds_client):
    snapshots_to_retain = 6
    snapshots = [_make_snapshot(i) for i in range(10)]
    snapshots_to_delete = snapshots[snapshots_to_retain:]
    rds_client.describe_db_snapshots.return_value = snapshots
    delete_previous_snapshots.function("fake_arn", snapshots_to_retain)
    rds_client.delete_db_snapshot.assert_has_calls(
        [
            mock.call(DBSnapshotIdentifier=snapshot["DBSnapshotIdentifier"])
            for snapshot in snapshots_to_delete
        ]
    )

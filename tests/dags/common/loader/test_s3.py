import os
import socket
from unittest.mock import patch
from urllib.parse import urlparse

import boto3
import pytest
from common.loader import s3


TEST_ID = "testing"
TEST_MEDIA_PREFIX = "media"
TEST_STAGING_PREFIX = "test_staging"
AWS_CONN_ID = os.getenv("AWS_CONN_ID")
S3_LOCAL_ENDPOINT = os.getenv("S3_LOCAL_ENDPOINT")
S3_TEST_BUCKET = f"cccatalog-storage-{TEST_ID}"
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
SECRET_KEY = os.getenv("AWS_SECRET_KEY")
S3_HOST = socket.gethostbyname(urlparse(S3_LOCAL_ENDPOINT).hostname)


@pytest.fixture
def empty_s3_bucket(socket_enabled):
    bucket = boto3.resource(
        "s3",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url=S3_LOCAL_ENDPOINT,
    ).Bucket(S3_TEST_BUCKET)

    def _delete_all_objects():
        key_list = [{"Key": obj.key} for obj in bucket.objects.all()]
        if len(list(bucket.objects.all())) > 0:
            bucket.delete_objects(Delete={"Objects": key_list})

    if bucket.creation_date:
        _delete_all_objects()
    else:
        bucket.create()
    yield bucket
    _delete_all_objects()


def test_copy_file_to_s3_staging_uses_connection_id():
    identifier = TEST_ID
    media_prefix = TEST_MEDIA_PREFIX
    staging_prefix = TEST_STAGING_PREFIX
    tsv_file_path = "/test/file/path/to/data.tsv"
    aws_conn_id = "test_conn_id"
    test_bucket_name = "test-bucket"

    with patch.object(s3, "S3Hook") as mock_s3:
        s3.copy_file_to_s3_staging(
            identifier,
            tsv_file_path,
            test_bucket_name,
            aws_conn_id,
            media_prefix=media_prefix,
            staging_prefix=staging_prefix,
        )
    mock_s3.assert_called_once_with(aws_conn_id=aws_conn_id)


def test_copy_file_to_s3_staging_uses_bucket_environ(monkeypatch):
    identifier = TEST_ID
    media_prefix = TEST_MEDIA_PREFIX
    staging_prefix = TEST_STAGING_PREFIX
    tsv_file_path = "/test/file/path/to/data.tsv"
    aws_conn_id = "test_conn_id"
    test_bucket_name = "test-bucket"
    monkeypatch.setenv("OPENVERSE_BUCKET", test_bucket_name)

    with patch.object(s3.S3Hook, "load_file") as mock_s3_load_file:
        s3.copy_file_to_s3_staging(
            identifier,
            tsv_file_path,
            test_bucket_name,
            aws_conn_id,
            media_prefix=media_prefix,
            staging_prefix=staging_prefix,
        )
    mock_s3_load_file.assert_called_once_with(
        tsv_file_path,
        f"{media_prefix}/{staging_prefix}/{identifier}/data.tsv",
        bucket_name=test_bucket_name,
    )


def test_copy_file_to_s3_staging_given_bucket_name():
    identifier = TEST_ID
    media_prefix = TEST_MEDIA_PREFIX
    staging_prefix = TEST_STAGING_PREFIX
    tsv_file_path = "/test/file/path/to/data.tsv"
    aws_conn_id = "test_conn_id"
    test_bucket_name = "test-bucket-given"

    with patch.object(s3.S3Hook, "load_file") as mock_s3_load_file:
        s3.copy_file_to_s3_staging(
            identifier,
            tsv_file_path,
            test_bucket_name,
            aws_conn_id,
            media_prefix=media_prefix,
            staging_prefix=staging_prefix,
        )
    print(mock_s3_load_file.mock_calls)
    print(mock_s3_load_file.method_calls)
    mock_s3_load_file.assert_called_once_with(
        tsv_file_path,
        f"{media_prefix}/{staging_prefix}/{identifier}/data.tsv",
        bucket_name=test_bucket_name,
    )


@pytest.mark.allow_hosts([S3_HOST])
def test_get_staged_s3_object_finds_object_with_defaults(empty_s3_bucket):
    media_prefix = s3.DEFAULT_MEDIA_PREFIX
    staging_prefix = s3.STAGING_PREFIX
    identifier = TEST_ID
    file_name = "data.tsv"
    test_key = "/".join([media_prefix, staging_prefix, identifier, file_name])
    empty_s3_bucket.put_object(Key=test_key)
    actual_key = s3.get_staged_s3_object(identifier, empty_s3_bucket.name, AWS_CONN_ID)
    assert actual_key == test_key


@pytest.mark.allow_hosts([S3_HOST])
def test_get_staged_s3_object_finds_object_with_givens(empty_s3_bucket):
    media_prefix = TEST_MEDIA_PREFIX
    staging_prefix = TEST_STAGING_PREFIX
    identifier = TEST_ID
    file_name = "data.tsv"
    test_key = "/".join([media_prefix, staging_prefix, identifier, file_name])
    empty_s3_bucket.put_object(Key=test_key)
    actual_key = s3.get_staged_s3_object(
        identifier,
        empty_s3_bucket.name,
        AWS_CONN_ID,
        media_prefix=media_prefix,
        staging_prefix=staging_prefix,
    )
    assert actual_key == test_key


@pytest.mark.allow_hosts([S3_HOST])
def test_get_staged_s3_object_complains_with_multiple_keys(empty_s3_bucket):
    media_prefix = TEST_MEDIA_PREFIX
    staging_prefix = TEST_STAGING_PREFIX
    identifier = TEST_ID
    file_1 = "data.tsv"
    file_2 = "datb.tsv"
    test_key_1 = "/".join([media_prefix, staging_prefix, identifier, file_1])
    test_key_2 = "/".join([media_prefix, staging_prefix, identifier, file_2])
    empty_s3_bucket.put_object(Key=test_key_1)
    empty_s3_bucket.put_object(Key=test_key_2)
    with pytest.raises(AssertionError):
        s3.get_staged_s3_object(
            identifier,
            empty_s3_bucket.name,
            AWS_CONN_ID,
            media_prefix=media_prefix,
            staging_prefix=staging_prefix,
        )

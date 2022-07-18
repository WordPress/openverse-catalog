import os
import socket
from unittest import mock
from urllib.parse import urlparse

import boto3
import pytest
from oauth2 import oauth2
from requests import Response


FAKE_OAUTH_PROVIDER_NAME = "fakeprovider"
S3_LOCAL_ENDPOINT = os.getenv("S3_LOCAL_ENDPOINT")
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
SECRET_KEY = os.getenv("AWS_SECRET_KEY")


def pytest_configure(config):
    """
    Dynamically allow the S3 host during testing. This is required because:
    * Docker will use different internal ports depending on what's available
    * Boto3 will open a socket with the IP address directly rather than the hostname
    * We can't add the allow_hosts mark to the empty_s3_bucket fixture directly
        (see: https://github.com/pytest-dev/pytest/issues/1368)
    """
    s3_host = socket.gethostbyname(urlparse(S3_LOCAL_ENDPOINT).hostname)
    config.__socket_allow_hosts = ["s3", "postgres", s3_host]


def _var_get_replacement(*args, **kwargs):
    values = {
        oauth2.OAUTH2_TOKEN_KEY: {
            FAKE_OAUTH_PROVIDER_NAME: {
                "access_token": "fakeaccess",
                "refresh_token": "fakerefresh",
            }
        },
        oauth2.OAUTH2_AUTH_KEY: {FAKE_OAUTH_PROVIDER_NAME: "fakeauthtoken"},
        oauth2.OAUTH2_PROVIDERS_KEY: {
            FAKE_OAUTH_PROVIDER_NAME: {
                "client_id": "fakeclient",
                "client_secret": "fakesecret",
            }
        },
    }
    return values[args[0]]


@pytest.fixture
def oauth_provider_var_mock():
    with mock.patch("oauth2.oauth2.Variable") as MockVariable:
        MockVariable.get.side_effect = _var_get_replacement
        yield MockVariable


def _make_response(*args, **kwargs):
    """
    Mock the request used during license URL validation. Most times the results of this
    function are expected to end with a `/`, so if the URL provided does not we add it.
    """
    response: Response = mock.Mock(spec=Response)
    if args:
        response.ok = True
        url = args[0]
        if isinstance(url, str) and not url.endswith("/"):
            url += "/"
        response.url = url
    return response


@pytest.fixture(autouse=True)
def requests_get_mock():
    """
    Mock request.get calls that occur during testing done by the
    `common.urls.rewrite_redirected_url` function.
    """
    with mock.patch("common.urls.requests_get", autospec=True) as mock_get:
        mock_get.side_effect = _make_response
        yield


@pytest.fixture
def freeze_time(monkeypatch):
    """
    Now() manager patches datetime return a fixed, settable, value
    (freezes time)

    https://stackoverflow.com/a/28073449 CC BY-SA 3.0
    """
    import datetime

    original = datetime.datetime

    class FreezeMeta(type):
        def __instancecheck__(self, instance):
            if type(instance) == original or type(instance) == Freeze:
                return True

    class Freeze(datetime.datetime):
        __metaclass__ = FreezeMeta

        @classmethod
        def freeze(cls, val):
            cls.frozen = val

        @classmethod
        def now(cls):
            return cls.frozen

        @classmethod
        def delta(cls, timedelta=None, **kwargs):
            """Moves time fwd/bwd by the delta"""
            from datetime import timedelta as td

            if not timedelta:
                timedelta = td(**kwargs)
            cls.frozen += timedelta

    monkeypatch.setattr(datetime, "datetime", Freeze)
    Freeze.freeze(original.now())
    return Freeze


def _delete_bucket(bucket):
    key_list = [{"Key": obj.key} for obj in bucket.objects.all()]
    if len(list(bucket.objects.all())) > 0:
        bucket.delete_objects(Delete={"Objects": key_list})
    bucket.delete()


@pytest.fixture
def empty_s3_bucket(request):
    # Bucket names can't be longer than 63 characters or have strange characters
    bucket_name = f"bucket-{hash(request.node.name)}"
    print(f"{bucket_name=}")
    bucket = boto3.resource(
        "s3",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        endpoint_url=S3_LOCAL_ENDPOINT,
    ).Bucket(bucket_name)

    if bucket.creation_date:
        _delete_bucket(bucket)
    bucket.create()
    yield bucket
    _delete_bucket(bucket)

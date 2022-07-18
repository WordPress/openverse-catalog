import base64
import os
import uuid
from pathlib import Path
from unittest import mock

import metrics.es_dashboard.dashboard_snapshots as ds
import metrics.es_dashboard.dashboard_snapshots_dag as ds_dag
import pytest


@pytest.mark.parametrize("metric", ["cpu", "network-data"])
def test_generate_widget_definition(metric):
    start = "2022-01-05"
    stop = "2022-01-08"
    with mock.patch(
        "metrics.es_dashboard.dashboard_snapshots.Variable"
    ) as VariableMock:
        instances = ["i666", "i1337", "i0"]
        VariableMock.get.return_value = ",".join(instances)
        actual = ds.generate_widget_definition.function(metric, start, stop)
        for expected in [*instances, ds.HEIGHT, ds.WIDTH, ds.RESOLUTION, start, stop]:
            assert (
                str(expected) in actual
            ), f"Could not find expected templated value '{expected}'"


def test_generate_png():
    with mock.patch(
        "metrics.es_dashboard.dashboard_snapshots.AwsBaseHook"
    ) as AwsBaseHookMock:
        mock_client = AwsBaseHookMock.return_value.get_client_type.return_value
        img_data = b"This is sample image data!"
        mock_client.get_metric_widget_image.return_value = {
            "MetricWidgetImage": img_data
        }
        actual = ds.generate_png.function("sample widget", "sample_conn_id")
        assert actual == base64.b64encode(img_data)


def _get_png_data(filename) -> bytes:
    png_path = Path(__file__).parent / filename
    return png_path.read_bytes()


def _get_image_blobs() -> list[bytes]:
    png_data = _get_png_data("tiny-png.png")
    return [base64.b64encode(png_data) for _ in range(2)]


def test_combine_images():
    image_blobs = _get_image_blobs()
    expected = _get_png_data("tiny-png-stacked.png")
    actual = ds.combine_images(image_blobs)
    assert actual.read() == expected, "Stacked image bytes did not match reference"


def test_upload_to_s3(empty_s3_bucket):
    key_prefix = str(uuid.uuid4())
    image_blobs = _get_image_blobs()
    s3_local_endpoint = os.getenv("S3_LOCAL_ENDPOINT")
    url = ds.upload_to_s3.function(
        image_blobs, empty_s3_bucket.name, key_prefix, ds_dag.S3_CONN_ID
    )
    assert s3_local_endpoint in url

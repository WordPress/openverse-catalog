"""
TODO:
    - docstring
    - mention which variables this uses
    - mention the alternate connection type for local dev
    - combine images together into single horizontal image
"""
import os
from datetime import datetime

from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
from common.constants import DAG_DEFAULT_ARGS
from metrics.es_dashboard.dashboard_snapshots import (
    generate_png,
    generate_widget_definition,
    send_message,
    upload_to_s3,
)


METRICS = ["cpu", "network-data"]
BUCKET = "openverse-airflow"
KEY_PREFIX = "metrics/elasticsearch/{{ ds }}"
AWS_METRICS_CONN_ID = os.environ.get("AWS_METRICS_CONN_ID", "aws_default")
S3_CONN_ID = os.environ.get("AWS_CONN_ID", "aws_default")


@dag(
    schedule_interval=None,
    default_args=DAG_DEFAULT_ARGS,
    start_date=datetime(2022, 7, 14),
    tags=["metrics"],
)
def elasticsearch_dashboard_snapshot():
    images = []
    start = "{{ macros.ds_add(ds, -3) }}"
    end = "{{ ds }}"
    for metric in METRICS:
        with TaskGroup(group_id=f"{metric.replace('-', '_')}_metrics"):
            templated_widget = generate_widget_definition(
                metric=metric,
                start=f"{start}T00:00:00.0000Z",
                end=f"{end}T00:00:00.0000Z",
            )
            png = generate_png(templated_widget, aws_conn_id=AWS_METRICS_CONN_ID)
            images.append(png)

    image_url = upload_to_s3(
        image_blobs=images,
        bucket=BUCKET,
        key_prefix=KEY_PREFIX,
        aws_conn_id=S3_CONN_ID,
    )
    send_message(image_url, start, end)


dashboard_snapshot = elasticsearch_dashboard_snapshot()

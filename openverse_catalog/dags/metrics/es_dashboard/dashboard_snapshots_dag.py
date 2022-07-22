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
    schedule_interval="@daily",
    default_args=DAG_DEFAULT_ARGS,
    start_date=datetime(2022, 7, 14),
    tags=["metrics"],
)
def elasticsearch_dashboard_snapshot():
    """
    # Elasticsearch Metrics Reporting DAG

    This DAG runs daily and is intended to provide a snapshot of Elasticsearch CPU and
    network activity. It does not perform anomaly detection and reports the status
    of the last 72 hours unconditionally. The time range for this DAG is based on the
    logical date for the DAG, meaning that reruns and backfills capture cluster
    performance for the logical DAG interval (**not** the actual DAG start time).

    ## Variables
    - `ES_INSTANCE_IDS` - _(Airflow Variable)_ a comma separated list of AWS EC2
    instance IDs which correspond to the Elasticsearch datanode instances which
    should be monitored.
    - `AWS_METRICS_CONN_ID` - _(Environment variable)_ AWS connection used for
    retrieving the ES metrics from CloudWatch (defaults to `AWS_CONN_ID` if not
    explicitly provided)

    The S3 and metrics connections are separated so developers can pull production
    metrics without uploading data to production S3.
    """
    images = []
    # We can't use `date_interval_end` here because ds_add requires a YYYY-MM-DD string.
    # The logical interval is 1 day, so in order to get 3 days we subtract 2 from the
    # start interval (`ds`)
    start = "{{ macros.ds_add(ds, -2) }}"
    end = "{{ data_interval_end | ds }}"
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

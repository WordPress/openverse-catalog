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
AWS_CONN_ID = os.environ.get("AWS_CONN_ID", "no_aws_conn")


@dag(
    schedule_interval=None,
    default_args=DAG_DEFAULT_ARGS,
    start_date=datetime(2022, 7, 1),
    catchup=False,
    tags=["metrics"],
)
def elasticsearch_dashboard_snapshot():
    images = []
    for metric in METRICS:
        with TaskGroup(group_id=f"{metric.replace('-', '_')}_metrics"):
            templated_widget = generate_widget_definition(metric)
            png = generate_png(templated_widget)
            image_info = upload_to_s3(
                metric=metric,
                image_data=png,
                bucket=BUCKET,
                key_prefix=KEY_PREFIX,
                aws_conn_id=AWS_CONN_ID,
            )
            images.append(image_info)

    send_message(images)


dashboard_snapshot = elasticsearch_dashboard_snapshot()

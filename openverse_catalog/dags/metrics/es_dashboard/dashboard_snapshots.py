import logging
from io import BytesIO
from pathlib import Path
from typing import NamedTuple

import boto3
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from common import slack


log = logging.getLogger(__name__)


class ImageData(NamedTuple):
    url: str
    title: str


@task()
def generate_widget_definition(metric: str) -> str:
    definition_path = Path(__file__).parent / f"widget_definitions/es-{metric}.json"
    definition_text = definition_path.read_text()

    es_instance_ids = Variable.get("ES_INSTANCE_IDS")
    es_node_1, es_node_2, es_node_3 = es_instance_ids.split(",")

    return (
        definition_text
        % {"es_node_1": es_node_1, "es_node_2": es_node_2, "es_node_3": es_node_3}
    ).replace("\n", "")


@task()
def generate_png(templated_widget: str) -> bytes:
    client = boto3.client("cloudwatch")
    widget_data = client.get_metric_widget_image(MetricWidget=templated_widget)
    # Note that this bytes output can be quite large, but it's worth storing in XComs so
    # the correct time window of data is stored in case the upload to S3 fails
    return widget_data["MetricWidgetImage"]


@task()
def upload_to_s3(
    metric: str, image_data: bytes, bucket: str, key_prefix: str, aws_conn_id: str
) -> ImageData:
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    bucket = s3.get_bucket(bucket)
    key = f"{key_prefix}/es-{metric}.png"
    bucket.upload_fileobj(BytesIO(image_data), key)
    log.info(f"Uploaded image data for {metric} to s3://{bucket}/{key}")
    url = s3.generate_presigned_url(
        client_method="get_object",
        params={"Bucket": bucket, "Key": key},
        expires_in=3600 * 24 * 7,  # 7 days
    )
    log.info(f"Presigned URL: {url}")
    return ImageData(url=url, title=f"{metric.title()} Usage")


@task()
def send_message(images: list[ImageData]):
    message = slack.SlackMessage(username="Cloudwatch Metrics", icon_emoji=":cloud:")
    text = "Elasticsearch metrics over the last 3 days"
    message.add_text(text)
    for image in images:
        message.add_image(url=image.url, title=image.title)
    message.send(notification_text=text)

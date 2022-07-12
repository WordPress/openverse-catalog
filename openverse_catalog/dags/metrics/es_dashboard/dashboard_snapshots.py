import base64
import logging
from io import BytesIO
from pathlib import Path
from typing import NamedTuple

from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
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
def generate_png(templated_widget: str, aws_conn_id: str) -> bytes:
    aws_hook = AwsBaseHook(aws_conn_id=aws_conn_id, client_type="cloudwatch")
    client = aws_hook.get_client_type()
    widget_data = client.get_metric_widget_image(MetricWidget=templated_widget)
    # Note that this bytes output can be quite large, but it's worth storing in XComs so
    # the correct time window of data is stored in case the upload to S3 fails
    return base64.b64encode(widget_data["MetricWidgetImage"])


@task()
def upload_to_s3(
    metric: str, image_data: bytes, bucket: str, key_prefix: str, aws_conn_id: str
) -> ImageData:
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    s3_bucket = s3.get_bucket(bucket)
    key = f"{key_prefix}/es-{metric}.png"
    decoded_data = BytesIO(base64.b64decode(image_data))
    s3_bucket.upload_fileobj(decoded_data, key)
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
    if slack.should_send_message():
        message.send(notification_text=text)
    else:
        image_text = "\n".join([f"{image.title}: {image.url}" for image in images])
        log.info("Skipping slack message")
        log.info(f"{text}\n{image_text}")

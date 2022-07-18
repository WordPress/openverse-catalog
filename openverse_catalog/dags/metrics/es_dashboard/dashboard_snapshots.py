import base64
import logging
from io import BytesIO
from pathlib import Path

import numpy as np
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from common import slack
from PIL import Image


log = logging.getLogger(__name__)


HEIGHT = 300
WIDTH = 1000
RESOLUTION = 900


@task()
def generate_widget_definition(metric: str, start: str, end: str) -> str:
    """
    Create the JSON widget definition required for AWS by rendering the template with
    the provided Elasticsearch instance IDs, image height & width, metric resolution,
    and time boundaries. The timeframe is provided by Airflow so backfills & reruns can
    be accurately performed.
    """
    definition_path = (
        Path(__file__).parent / f"widget_definitions/es-{metric}.json.template"
    )
    definition_text = definition_path.read_text()

    es_instance_ids = Variable.get("ES_INSTANCE_IDS")
    es_node_1, es_node_2, es_node_3 = es_instance_ids.split(",")

    return (
        # Need to use % formatting because JSON has a ton of brackets in it which
        # get misinterpreted as strings to format
        definition_text
        % {
            "es_node_1": es_node_1,
            "es_node_2": es_node_2,
            "es_node_3": es_node_3,
            "height": HEIGHT,
            "width": WIDTH,
            "period": RESOLUTION,
            "start": start,
            "end": end,
        }
    ).replace("\n", "")


@task()
def generate_png(templated_widget: str, aws_conn_id: str) -> bytes:
    """
    Produce a base64 encoded PNG image using the templated widget definition and the
    provided AWS connection. The PNG is returned as bytes and is base64 encoded to
    preserve space in the XComs.
    """
    aws_hook = AwsBaseHook(aws_conn_id=aws_conn_id, client_type="cloudwatch")
    client = aws_hook.get_client_type()
    widget_data = client.get_metric_widget_image(MetricWidget=templated_widget)
    # Note that this bytes output can be quite large, but it's worth storing in XComs so
    # the correct time window of data is stored in case the upload to S3 fails
    return base64.b64encode(widget_data["MetricWidgetImage"])


def combine_images(image_blobs: list[bytes]) -> BytesIO:
    """
    Combine the provided images by stacking them vertically. This method takes base64
    encoded image blobs and uses numpy to stack each array vertically. It returns a
    BytesIO in-memory file pointer to the generated PNG.
    """
    # Combine the images vertically
    # From https://stackoverflow.com/a/30228789 via 'dermen' CC BY-SA 4.0
    images = [
        # Decode each image from base64 then open as a Pillow Image
        Image.open(BytesIO(base64.b64decode(image_blob)))
        for image_blob in image_blobs
    ]
    combined = np.vstack([np.asarray(image) for image in images])
    output = BytesIO()
    Image.fromarray(combined).save(output, format="PNG")
    # Close all the associated image files
    for image in images:
        image.close()
    # Seek back to the beginning of the file so S3 can upload it properly
    output.seek(0)
    return output


@task()
def upload_to_s3(
    image_blobs: list[bytes],
    bucket: str,
    key_prefix: str,
    aws_conn_id: str,
) -> str:
    """
    Uploads the provided base64 encoded image blobs to S3 after combining them
    vertically. This function returns a presigned URL that can then be consumed by
    external clients (e.g. Slack) for 7 days.
    """
    combined_image = combine_images(image_blobs)
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    s3_bucket = s3.get_bucket(bucket)
    key = f"{key_prefix}/es-metrics.png"
    s3_bucket.upload_fileobj(combined_image, key)
    log.info(f"Uploaded elasticsearch metrics image to s3://{bucket}/{key}")
    url = s3.generate_presigned_url(
        client_method="get_object",
        params={"Bucket": bucket, "Key": key},
        expires_in=3600 * 24 * 7,  # 7 days
    )
    log.info(f"Presigned URL: {url}")
    return url


@task()
def send_message(image_url: str, start: str, end: str):
    """
    Send a message to Slack with the provided image URL.
    """
    message = slack.SlackMessage(username="Cloudwatch Metrics", icon_emoji=":cloud:")
    text = f"Elasticsearch metrics from {start} to {end}"
    message.add_text(text)
    message.add_image(url=image_url, title="Elasticsearch Usage")
    if slack.should_send_message():
        message.send(notification_text=text)
    else:
        log.info("Skipping slack message")
        log.info(f"{text}\n{image_url}")

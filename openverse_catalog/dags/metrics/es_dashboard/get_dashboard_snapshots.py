import json
from pathlib import Path

from airflow.models import Variable
from common import slack


def generate_metric_data(definition: str) -> str:
    definition_path = Path(__file__).parent / f"widget_definitions/{definition}.json"
    definition_text = definition_path.read_text()

    es_instance_ids = Variable.get("ES_INSTANCE_IDS")
    es_node_1, es_node_2, es_node_3 = es_instance_ids.split(",")

    return definition_text.format(
        es_node_1=es_node_1, es_node_2=es_node_2, es_node_3=es_node_3
    ).replace("\n", "")


def generate_png(metric_data: dict, client) -> str:
    metric_widget = json.dumps(metric_data)
    widget_data = client.get_metric_widget_image(MetricWidget=metric_widget)
    return widget_data["MetricWidgetImage"].encode("utf-8")


# def generate_png(metric_data: dict, output_dir: Path, client) -> Path:
#     widget_name = metric_data["title"].replace(":", "").lower().replace(" ", "_")
#     widget_name += f"_{date.today().isoformat()}.png"
#     metric_widget = json.dumps(metric_data)
#     widget_data = client.get_metric_widget_image(MetricWidget=metric_widget)
#     output_file = output_dir / widget_name
#     output_file.write_bytes(base64.decodebytes(widget_data["MetricWidgetImage"]))
#     return output_file


def send_message(images: list[str]):
    message = slack.SlackMessage(username="Cloudwatch Metrics", icon_emoji=":cloud:")
    message.add_text("Elasticsearch metrics over the last 3 days")
    for image in images:
        message.add_image(url="data:image/png;")


def main():
    ...

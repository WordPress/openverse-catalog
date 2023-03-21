import json
from pathlib import Path


def get_resource_json(folder_name, json_name):

    RESOURCES = Path(__file__).parent / folder_name
    return json.loads((RESOURCES / json_name).read_text())

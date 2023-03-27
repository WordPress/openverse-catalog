import json
from collections.abc import Callable
from pathlib import Path


def make_resource_json_func(folder_name: str) -> Callable[[str], dict]:
    """
    _summary_

    Args:
        folder_name (str): _description_

    Returns:
        Callable[[str], dict]: A function that when given a json
        resource name as a string returns it as a dictionary.
    """
    resources = Path(__file__).parent / folder_name

    def get_resource_json(resource_name: str) -> dict:
        return json.loads((resources / resource_name).read_text())

    return get_resource_json

import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests
from common.licenses import LicenseInfo
from providers.provider_api_scripts.metropolitan_museum import MetMuseumDataIngester


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

mma = MetMuseumDataIngester()

RESOURCES = Path(__file__).parent / "resources/metropolitan_museum_of_art"
print(RESOURCES)

# abbreviated response without other images 45733
single_object_response = json.loads(
    (RESOURCES / "sample_response_without_additional.json").read_text()
)
# single expected record if 45733 with no additional images
single_expected_data = json.loads((RESOURCES / "sample_image_data.json").read_text())

# response for objectid 45734 with 2 additional image urls
full_object_response = json.loads((RESOURCES / "sample_response.json").read_text())
# 3 expected image records for objectid 45734
full_expected_data = json.loads(
    (RESOURCES / "sample_additional_image_data.json").read_text()
)

# Not CC0
ineligible_response = json.loads(
    (RESOURCES / "sample_response_1027_not_cc0.json").read_text()
)


CC0 = LicenseInfo(
    "cc0", "1.0", "https://creativecommons.org/publicdomain/zero/1.0/", None
)


@pytest.mark.parametrize(
    "test_date, expected",
    [("2022-07-01", {"metadataDate": "2022-07-01"}), (None, None)],
)
def test_get_next_query_params(test_date, expected):
    ingester = MetMuseumDataIngester(test_date)
    actual = ingester.get_next_query_params()
    assert actual == expected


@pytest.mark.parametrize(
    "response_json, expected",
    [
        ({"total": 4, "objectIDs": [153, 1578, 465, 546]}, [153, 1578, 465, 546]),
        (None, None),
    ],
)
def test_get_batch_data(response_json, expected):
    actual = mma.get_batch_data(response_json)
    assert actual == expected


@pytest.mark.parametrize(
    "case_name, response_json, expected",
    [
        (
            "single image",
            single_object_response,
            single_expected_data[0].get("meta_data"),
        ),
        ("multi images", full_object_response, full_expected_data[0].get("meta_data")),
    ],
)
def test_create_meta_data(case_name, response_json, expected):
    actual = mma._create_meta_data(response_json)
    assert expected == actual


@pytest.mark.parametrize(
    "case_name, response_json, expected",
    [
        (
            "single image",
            single_object_response,
            single_expected_data[0].get("raw_tags"),
        ),
        ("multi images", full_object_response, full_expected_data[0].get("raw_tags")),
    ],
)
def test_create_tag_list(case_name, response_json, expected):
    actual = mma._create_tag_list(response_json)
    assert expected == actual


def test_get_record_data_with_none_response():
    with patch.object(mma.delayed_requester, "get", return_value=None) as mock_get:
        with pytest.raises(Exception):
            assert mma.get_record_data(10)
    assert mock_get.call_count == 6


def test_get_record_data_with_non_ok():
    r = requests.Response()
    r.status_code = 504
    r.json = MagicMock(return_value={})
    with patch.object(mma.delayed_requester, "get", return_value=r) as mock_get:
        with pytest.raises(Exception):
            assert mma.get_record_data(10)
    assert mock_get.call_count == 6


@pytest.mark.parametrize(
    "case_name, response_json, expected",
    [
        ("single image", single_object_response, single_expected_data),
        ("multi image", full_object_response, full_expected_data),
        ("not cc0", ineligible_response, None),
    ],
)
def test_get_record_data_returns_response_json_when_all_ok(
    case_name, response_json, expected, monkeypatch
):
    monkeypatch.setattr(
        mma.delayed_requester, "get_response_json", lambda x, y: response_json
    )
    actual = mma.get_record_data(response_json.get("objectID"))

    if expected is None:
        assert actual is None
    else:
        assert len(actual) == len(expected)
        for i in range(len(actual)):
            for key, value in expected[i].items():
                if key == "license_info":
                    assert actual[i].get(key) == CC0
                else:
                    assert actual[i].get(key) == value

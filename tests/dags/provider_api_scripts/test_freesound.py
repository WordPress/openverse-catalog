import json
import logging
from pathlib import Path
from unittest.mock import patch

import freesound
from common.licenses.licenses import LicenseInfo

RESOURCES = Path(__file__).parent / "tests/resources/freesound"
AUDIO_DATA_EXAMPLE = RESOURCES / "audio_data_example.json"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s",
    level=logging.DEBUG,
)

freesound._get_set_name = lambda x: x


def test_get_audio_pages_returns_correctly_with_none_json():
    expect_result = None
    with patch.object(
        freesound.delayed_requester, "get_response_json", return_value=None
    ):
        actual_result = freesound._get_batch_json()
    assert actual_result == expect_result


def test_get_audio_pages_returns_correctly_with_no_results():
    expect_result = None
    with patch.object(
        freesound.delayed_requester, "get_response_json", return_value={}
    ):
        actual_result = freesound._get_batch_json()
    assert actual_result == expect_result


def test_get_query_params_adds_page_number():
    actual_qp = freesound._get_query_params(page_number=2)
    assert actual_qp["page"] == str(2)


def test_get_query_params_leaves_other_keys():
    actual_qp = freesound._get_query_params(
        page_number=200, default_query_params={"test": "value"}
    )
    assert actual_qp["test"] == "value"
    assert len(actual_qp.keys()) == 3


def test_get_items():
    with open(RESOURCES / "page.json") as f:
        first_response = json.load(f)
    with patch.object(freesound, "_get_batch_json", side_effect=[first_response, []]):
        expected_audio_count = 6
        actual_audio_count = freesound._get_items(license_name="Attribution")
        assert expected_audio_count == actual_audio_count


def test_process_item_batch_handles_example_batch():
    with open(AUDIO_DATA_EXAMPLE) as f:
        items_batch = [json.load(f)]
    with patch.object(freesound.audio_store, "add_item", return_value=1) as mock_add:
        freesound._process_item_batch(items_batch)
        mock_add.assert_called_once()
        _, actual_call_args = mock_add.call_args_list[0]
        expected_call_args = {
            "alt_files": [
                {
                    "bit_rate": 1381,
                    "filesize": 107592,
                    "format": "wav",
                    "sample_rate": 44100.0,
                    "url": "https://freesound.org/apiv2/sounds/415362/download/",
                },
                {
                    "format": "ogg",
                    "url": "https://freesound.org/data/previews/415/415362_6044691-lq.ogg",
                },
                {
                    "format": "mp3",
                    "url": "https://freesound.org/data/previews/415/415362_6044691-lq.mp3",
                },
                {
                    "format": "ogg",
                    "url": "https://freesound.org/data/previews/415/415362_6044691-hq.ogg",
                },
                {
                    "format": "mp3",
                    "url": "https://freesound.org/data/previews/415/415362_6044691-hq.mp3",
                },
            ],
            # To avoid making API requests during tests, we return the URL
            # instead of querying API for audio set name
            "audio_set": "https://freesound.org/apiv2/packs/23434/",
            "audio_url": "https://freesound.org/people/owly-bee/sounds/415362/",
            "bit_rate": 1381,
            "category": "sound",
            "creator": "owly-bee",
            "creator_url": "https://freesound.org/people/owly-bee/",
            "duration": 608,
            "foreign_identifier": 415362,
            "foreign_landing_url": "https://freesound.org/people/owly-bee/sounds/415362/",
            "license_info": LicenseInfo(
                license="by",
                version="3.0",
                url="https://creativecommons.org/licenses/by/3.0/",
                raw_url="http://creativecommons.org/licenses/by/3.0/",
            ),
            "meta_data": {
                "description": "A disinterested noise in a somewhat low tone.",
                "download": "https://freesound.org/apiv2/sounds/415362/download/",
                "num_downloads": 164,
                "previews": {
                    "preview-hq-mp3": "https://freesound.org/data/previews/415/415362_6044691-hq.mp3",
                    "preview-hq-ogg": "https://freesound.org/data/previews/415/415362_6044691-hq.ogg",
                    "preview-lq-mp3": "https://freesound.org/data/previews/415/415362_6044691-lq.mp3",
                    "preview-lq-ogg": "https://freesound.org/data/previews/415/415362_6044691-lq.ogg",
                },
            },
            "raw_tags": ["eh", "disinterest", "low", "uh", "voice", "uncaring"],
            "sample_rate": 44100,
            "set_url": "https://freesound.org/apiv2/packs/23434/",
            "title": "Ehh disinterested.wav",
        }
        assert actual_call_args == expected_call_args


def test_extract_audio_data_returns_none_when_media_data_none():
    actual_audio_info = freesound._extract_audio_data(None)
    expected_audio_info = None
    assert actual_audio_info is expected_audio_info


def test_extract_audio_data_returns_none_when_no_foreign_id():
    with open(AUDIO_DATA_EXAMPLE) as f:
        audio_data = json.load(f)
        audio_data.pop("id", None)
    actual_audio_info = freesound._extract_audio_data(audio_data)
    expected_audio_info = None
    assert actual_audio_info is expected_audio_info


def test_extract_audio_data_returns_none_when_no_audio_url():
    with open(AUDIO_DATA_EXAMPLE) as f:
        audio_data = json.load(f)
        audio_data.pop("url", None)
        audio_data.pop("download", None)
    actual_audio_info = freesound._extract_audio_data(audio_data)
    assert actual_audio_info is None


def test_extract_audio_data_returns_none_when_no_license():
    with open(AUDIO_DATA_EXAMPLE) as f:
        audio_data = json.load(f)
        audio_data.pop("license", None)
    actual_audio_info = freesound._extract_audio_data(audio_data)
    assert actual_audio_info is None


def test_get_audio_set_info():
    with open(AUDIO_DATA_EXAMPLE) as f:
        audio_data = json.load(f)
    audio_set, set_url = freesound._get_audio_set(audio_data)
    expected_audio_set_info = (
        "Opera I",
        6,
        "https://www.freesound.com/album/119/opera-i",
        "https://usercontent.freesound.com?type=album&id=119&width=200&trackid=732",
    )
    assert audio_set, set_url == expected_audio_set_info


def test_get_creator_data():
    with open(AUDIO_DATA_EXAMPLE) as f:
        audio_data = json.load(f)
    actual_creator, actual_creator_url = freesound._get_creator_data(audio_data)
    expected_creator = "owly-bee"
    expected_creator_url = "https://freesound.org/people/owly-bee/"

    assert actual_creator == expected_creator
    assert actual_creator_url == expected_creator_url


def test_get_creator_data_returns_none_when_no_artist():
    with open(AUDIO_DATA_EXAMPLE) as f:
        audio_data = json.load(f)
    audio_data.pop("username", None)
    actual_creator, actual_creator_url = freesound._get_creator_data(audio_data)

    assert actual_creator is None
    assert actual_creator_url is None


def test_extract_audio_data_handles_example_dict():
    with open(AUDIO_DATA_EXAMPLE) as f:
        audio_data = json.load(f)

    actual_audio_info = freesound._extract_audio_data(audio_data)
    preview_url_base = "https://freesound.org/data/previews/415"
    expected_audio_info = {
        "alt_files": [
            {
                "bit_rate": 1381,
                "filesize": 107592,
                "format": "wav",
                "sample_rate": 44100.0,
                "url": "https://freesound.org/apiv2/sounds/415362/download/",
            },
            {"format": "ogg", "url": f"{preview_url_base}/415362_6044691-lq.ogg"},
            {"format": "mp3", "url": f"{preview_url_base}/415362_6044691-lq.mp3"},
            {"format": "ogg", "url": f"{preview_url_base}/415362_6044691-hq.ogg"},
            {"format": "mp3", "url": f"{preview_url_base}/415362_6044691-hq.mp3"},
        ],
        "audio_set": "https://freesound.org/apiv2/packs/23434/",
        "audio_url": "https://freesound.org/people/owly-bee/sounds/415362/",
        "bit_rate": 1381,
        "category": "sound",
        "creator": "owly-bee",
        "creator_url": "https://freesound.org/people/owly-bee/",
        "duration": 608,
        "foreign_identifier": 415362,
        "foreign_landing_url": "https://freesound.org/people/owly-bee/sounds/415362/",
        "license_info": LicenseInfo(
            license="by",
            version="3.0",
            url="https://creativecommons.org/licenses/by/3.0/",
            raw_url="http://creativecommons.org/licenses/by/3.0/",
        ),
        "meta_data": {
            "description": "A disinterested noise in a somewhat low tone.",
            "download": "https://freesound.org/apiv2/sounds/415362/download/",
            "num_downloads": 164,
            "previews": {
                "preview-hq-mp3": f"{preview_url_base}/415362_6044691-hq.mp3",
                "preview-hq-ogg": f"{preview_url_base}/415362_6044691-hq.ogg",
                "preview-lq-mp3": f"{preview_url_base}/415362_6044691-lq.mp3",
                "preview-lq-ogg": f"{preview_url_base}/415362_6044691-lq.ogg",
            },
        },
        "raw_tags": ["eh", "disinterest", "low", "uh", "voice", "uncaring"],
        "sample_rate": 44100,
        "set_url": "https://freesound.org/apiv2/packs/23434/",
        "title": "Ehh disinterested.wav",
    }
    assert actual_audio_info == expected_audio_info


def test_get_tags():
    with open(AUDIO_DATA_EXAMPLE) as f:
        audio_data = json.load(f)

    item_data = freesound._extract_audio_data(audio_data)
    actual_tags = item_data["raw_tags"]
    expected_tags = ["eh", "disinterest", "low", "uh", "voice", "uncaring"]
    assert expected_tags == actual_tags

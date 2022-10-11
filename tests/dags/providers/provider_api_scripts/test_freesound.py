import json
from pathlib import Path
from unittest.mock import patch

import pytest
from common.licenses.licenses import LicenseInfo
from providers.provider_api_scripts.freesound import FreesoundDataIngester


RESOURCES = Path(__file__).parent.resolve() / "resources/freesound"
fsd = FreesoundDataIngester()
AUDIO_FILE_SIZE = 16359


@pytest.fixture(autouse=True)
def freesound_module():
    old_get_set = fsd._get_set_info
    fsd._get_set_info = lambda x: ("foo", x)
    yield
    fsd._get_set_info = old_get_set


@pytest.fixture
def file_size_patch():
    with patch.object(fsd, "_get_audio_file_size") as get_file_size_mock:
        get_file_size_mock.return_value = AUDIO_FILE_SIZE
        yield


@pytest.fixture
def audio_data():
    AUDIO_DATA_EXAMPLE = RESOURCES / "audio_data_example.json"
    with open(AUDIO_DATA_EXAMPLE) as f:
        yield json.load(f)


def test_get_audio_pages_returns_correctly_with_no_data():
    actual_result = fsd.get_batch_data({})
    assert actual_result is None


def test_get_audio_pages_returns_correctly_with_empty_list():
    expect_result = []
    actual_result = fsd.get_batch_data({"results": [None, None, None]})
    assert actual_result == expect_result


@pytest.mark.parametrize(
    "exception_type",
    [
        # These are fine
        *FreesoundDataIngester.flaky_exceptions,
        # This should raise immediately
        pytest.param(ValueError, marks=pytest.mark.raises(exception=ValueError)),
    ],
)
def test_get_audio_file_size_retries_and_does_not_raise(exception_type, audio_data):
    expected_result = None
    # Patch the sleep function so it doesn't take long
    with patch("requests.head") as head_patch, patch("time.sleep"):
        head_patch.side_effect = exception_type("whoops")
        actual_result = fsd.get_record_data(audio_data)

        assert head_patch.call_count == 3
        assert actual_result == expected_result


def test_get_query_params_increments_page_number():
    first_qp = fsd.get_next_query_params(None)
    second_qp = fsd.get_next_query_params(first_qp)
    first_page = first_qp.pop("page")
    second_page = second_qp.pop("page")
    # Should be the same beyond that
    assert first_qp == second_qp
    assert second_page == first_page + 1


def test_get_items(file_size_patch):
    with open(RESOURCES / "page.json") as f:
        first_response = json.load(f)
    expected_audio_count = 6
    actual_audio_count = fsd.process_batch(first_response)
    assert actual_audio_count == expected_audio_count


def test_process_item_batch_handles_example_batch(audio_data, file_size_patch):
    items_batch = [audio_data]
    with patch.object(
        fsd.media_stores["audio"], "add_item", return_value=1
    ) as mock_add:
        fsd.process_batch(items_batch)
        mock_add.assert_called_once()
        _, actual_call_args = mock_add.call_args_list[0]
        expected_call_args = {
            "alt_files": [
                {
                    "bit_rate": 1381000,
                    "filesize": 107592,
                    "filetype": "wav",
                    "sample_rate": 44100,
                    "url": "https://freesound.org/apiv2/sounds/415362/download/",
                }
            ],
            "audio_set": "https://freesound.org/apiv2/packs/23434/",
            "audio_url": "https://freesound.org/data/previews/415/415362_6044691-hq.mp3",
            "bit_rate": 128000,
            "creator": "owly-bee",
            "creator_url": "https://freesound.org/people/owly-bee/",
            "duration": 608,
            "filesize": AUDIO_FILE_SIZE,
            "filetype": "mp3",
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
            },
            "raw_tags": ["eh", "disinterest", "low", "uh", "voice", "uncaring"],
            "set_foreign_id": "foo",
            "set_url": "https://freesound.org/apiv2/packs/23434/",
            "title": "Ehh disinterested.wav",
        }
        assert actual_call_args == expected_call_args


def test_extract_audio_data_returns_none_when_no_foreign_id(audio_data):
    audio_data.pop("id", None)
    actual_audio_info = fsd.get_record_data(audio_data)
    expected_audio_info = None
    assert actual_audio_info is expected_audio_info


def test_extract_audio_data_returns_none_when_no_audio_url(audio_data):
    audio_data.pop("url", None)
    audio_data.pop("download", None)
    actual_audio_info = fsd.get_record_data(audio_data)
    assert actual_audio_info is None


def test_extract_audio_data_returns_none_when_no_license(audio_data):
    audio_data.pop("license", None)
    actual_audio_info = fsd.get_record_data(audio_data)
    assert actual_audio_info is None


def test_get_audio_set_info(audio_data):
    set_foreign_id, audio_set, set_url = fsd._get_audio_set_info(audio_data)
    expected_audio_set_info = (
        "foo",
        "https://freesound.org/apiv2/packs/23434/",
        "https://freesound.org/apiv2/packs/23434/",
    )
    assert (set_foreign_id, audio_set, set_url) == expected_audio_set_info


def test_get_creator_data(audio_data):
    actual_creator, actual_creator_url = fsd._get_creator_data(audio_data)
    expected_creator = "owly-bee"
    expected_creator_url = "https://freesound.org/people/owly-bee/"

    assert actual_creator == expected_creator
    assert actual_creator_url == expected_creator_url


def test_get_creator_data_returns_none_when_no_artist(audio_data):
    audio_data.pop("username", None)
    actual_creator, actual_creator_url = fsd._get_creator_data(audio_data)

    assert actual_creator is None
    assert actual_creator_url is None


def test_extract_audio_data_handles_example_dict(audio_data, file_size_patch):
    actual_audio_info = fsd.get_record_data(audio_data)
    expected_audio_info = {
        "alt_files": [
            {
                "bit_rate": 1381000,
                "filesize": 107592,
                "filetype": "wav",
                "sample_rate": 44100,
                "url": "https://freesound.org/apiv2/sounds/415362/download/",
            },
        ],
        "audio_set": "https://freesound.org/apiv2/packs/23434/",
        "audio_url": "https://freesound.org/data/previews/415/415362_6044691-hq.mp3",
        "bit_rate": 128000,
        "creator": "owly-bee",
        "creator_url": "https://freesound.org/people/owly-bee/",
        "duration": 608,
        "filesize": AUDIO_FILE_SIZE,
        "filetype": "mp3",
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
        },
        "raw_tags": ["eh", "disinterest", "low", "uh", "voice", "uncaring"],
        "set_foreign_id": "foo",
        "set_url": "https://freesound.org/apiv2/packs/23434/",
        "title": "Ehh disinterested.wav",
    }
    assert actual_audio_info == expected_audio_info


def test_get_tags(audio_data, file_size_patch):
    item_data = fsd.get_record_data(audio_data)
    actual_tags = item_data["raw_tags"]
    expected_tags = ["eh", "disinterest", "low", "uh", "voice", "uncaring"]
    assert expected_tags == actual_tags

import logging
from unittest.mock import patch

import pytest
import storage.columns
from common.licenses.licenses import LicenseInfo
from storage import audio


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s", level=logging.DEBUG
)

logger = logging.getLogger(__name__)

PD_LICENSE_INFO = LicenseInfo(
    "zero", "1.0", "https://creativecommons.org/publicdomain/zero/1.0/", None
)
BY_LICENSE_INFO = LicenseInfo(
    "by", "4.0", "https://creativecommons.org/licenses/by/4.0/", None
)

mock_audio_args = {
    "foreign_landing_url": "https://landing_page.com",
    "audio_url": "https://audiourl.com",
    "license_info": BY_LICENSE_INFO,
    "foreign_identifier": "foreign_id",
    "thumbnail_url": "https://thumbnail.com",
    "duration": 200,
    "creator": "tyler",
    "creator_url": "https://creatorurl.com",
    "title": "agreatsong",
    "meta_data": {},
    "raw_tags": {},
    "watermarked": None,
    "bit_rate": None,
    "sample_rate": None,
    "category": None,
    "genres": [],
    "audio_set": {},
    "alt_files": [],
    "source": "testing_source",
    "ingestion_type": "provider_api",
}


@pytest.fixture
def setup_env(monkeypatch):
    monkeypatch.setenv("OUTPUT_DIR", "/tmp")


def test_AudioStore_includes_provider_in_output_file_string():
    audio_store = audio.AudioStore("test_provider")
    assert type(audio_store._OUTPUT_PATH) == str
    assert "test_provider" in audio_store._OUTPUT_PATH


def test_AudioStore_add_item_adds_realistic_audio_to_buffer():
    license_info = PD_LICENSE_INFO
    audio_store = audio.AudioStore(provider="testing_provider")
    audio_store.add_item(
        foreign_identifier="01",
        foreign_landing_url="https://audios.org/audio01",
        audio_url="https://audios.org/audio01.jpg",
        license_info=license_info,
        ingestion_type="provider_api",
    )
    assert len(audio_store._media_buffer) == 1


def test_AudioStore_add_item_adds_multiple_audios_to_buffer():
    audio_store = audio.AudioStore(provider="testing_provider")
    audio_store.add_item(
        foreign_identifier="01",
        foreign_landing_url="https://audios.org/audio01",
        audio_url="https://audios.org/audio01.jpg",
        license_info=PD_LICENSE_INFO,
    )
    audio_store.add_item(
        foreign_identifier="02",
        foreign_landing_url="https://audios.org/audio02",
        audio_url="https://audios.org/audio02.jpg",
        license_info=PD_LICENSE_INFO,
    )
    audio_store.add_item(
        foreign_identifier="03",
        foreign_landing_url="https://audios.org/audio03",
        audio_url="https://audios.org/audio03.jpg",
        license_info=PD_LICENSE_INFO,
    )
    audio_store.add_item(
        foreign_identifier="04",
        foreign_landing_url="https://audios.org/audio04",
        audio_url="https://audios.org/audio04.jpg",
        license_info=PD_LICENSE_INFO,
    )
    assert len(audio_store._media_buffer) == 4


def test_AudioStore_add_item_flushes_buffer(tmpdir):
    output_file = "testing.tsv"
    tmp_directory = tmpdir
    output_dir = str(tmp_directory)
    tmp_file = tmp_directory.join(output_file)
    tmp_path_full = str(tmp_file)
    audio_store = audio.AudioStore(
        provider="testing_provider",
        output_file=output_file,
        output_dir=output_dir,
        buffer_length=3,
    )
    audio_store.add_item(
        foreign_identifier="01",
        foreign_landing_url="https://audios.org/audio01",
        audio_url="https://audios.org/audio01.jpg",
        license_info=PD_LICENSE_INFO,
    )
    audio_store.add_item(
        foreign_identifier="02",
        foreign_landing_url="https://audios.org/audio02",
        audio_url="https://audios.org/audio02.jpg",
        license_info=PD_LICENSE_INFO,
    )
    audio_store.add_item(
        foreign_identifier="03",
        foreign_landing_url="https://audios.org/audio03",
        audio_url="https://audios.org/audio03.jpg",
        license_info=PD_LICENSE_INFO,
    )
    audio_store.add_item(
        foreign_identifier="04",
        foreign_landing_url="https://audios.org/audio04",
        audio_url="https://audios.org/audio04.jpg",
        license_info=PD_LICENSE_INFO,
    )
    assert len(audio_store._media_buffer) == 1
    with open(tmp_path_full) as f:
        lines = f.read().split("\n")
    assert len(lines) == 4  # recall the last '\n' will create an empty line.


def test_AudioStore_commit_writes_nothing_if_no_lines_in_buffer():
    audio_store = audio.AudioStore(output_dir="/path/does/not/exist")
    audio_store.commit()


def test_AudioStore_produces_correct_total_audios():
    audio_store = audio.AudioStore(provider="testing_provider")
    audio_store.add_item(
        foreign_identifier="01",
        foreign_landing_url="https://audios.org/audio01",
        audio_url="https://audios.org/audio01.jpg",
        license_info=PD_LICENSE_INFO,
    )
    audio_store.add_item(
        foreign_identifier="02",
        foreign_landing_url="https://audios.org/audio02",
        audio_url="https://audios.org/audio02.jpg",
        license_info=PD_LICENSE_INFO,
    )
    audio_store.add_item(
        foreign_identifier="03",
        foreign_landing_url="https://audios.org/audio03",
        audio_url="https://audios.org/audio03.jpg",
        license_info=PD_LICENSE_INFO,
    )
    assert audio_store.total_items == 3


def test_AudioStore_get_audio_enriches_multiple_tags():
    audio_store = audio.AudioStore("test_provider")
    audio_args = mock_audio_args.copy()
    audio_args["raw_tags"] = ["tagone", "tag2", "tag3"]
    actual_audio = audio_store._get_audio(
        **audio_args,
    )

    assert actual_audio.tags == [
        {"name": "tagone", "provider": "test_provider"},
        {"name": "tag2", "provider": "test_provider"},
        {"name": "tag3", "provider": "test_provider"},
    ]


@pytest.fixture
def default_audio_args():
    return dict(
        foreign_identifier="foreign_id",
        foreign_landing_url="https://landing_page.org",
        url="https://audiourl.org",
        thumbnail_url="https://thumbnail.com",
        filesize=None,
        audio_set=None,
        license_="by",
        license_version="4.0",
        creator="tyler",
        creator_url="https://creatorurl.com",
        title="agreatsong",
        meta_data={"description": "cat song"},
        tags={"name": "tag1", "provider": "testing"},
        watermarked=None,
        duration=100,
        bit_rate=None,
        sample_rate=None,
        category="music",
        genres=["rock", "pop"],
        alt_files=None,
        provider="testing_provider",
        source="testing_source",
        ingestion_type="provider_api",
    )


def test_create_tsv_row_creates_alt_files(
    default_audio_args,
):
    audio_store = audio.AudioStore()
    audio_args = default_audio_args.copy()
    alt_files = [
        {
            "url": "https://alternative.com/audio.mp3",
            "filesize": 123,
            "bit_rate": 41000,
            "sample_rate": "16000",
        }
    ]
    audio_args["alt_files"] = alt_files
    test_audio = audio.Audio(**audio_args)

    def mock_url_validator(value):
        # This avoids needing the internet for testing.
        return value

    with patch.object(
        storage.columns.urls,
        "validate_url_string",
        side_effect=mock_url_validator,
    ):
        actual_row = audio_store._create_tsv_row(test_audio)
        expected_row = (
            "\t".join(
                [
                    "foreign_id",
                    "https://landing_page.org",
                    "https://audiourl.org",
                    "https://thumbnail.com",
                    "\\N",
                    "by",
                    "4.0",
                    "tyler",
                    "https://creatorurl.com",
                    "agreatsong",
                    '{"description": "cat song"}',
                    '{"name": "tag1", "provider": "testing"}',
                    "\\N",
                    "testing_provider",
                    "testing_source",
                    "provider_api",
                    "100",
                    "\\N",
                    "\\N",
                    "music",
                    '{"rock", "pop"}',
                    "\\N",
                    '[{"url": '
                    '"https://alternative.com/audio.mp3", "filesize": "123", "bit_rate": "41000", '
                    '"sample_rate": "16000"}]',
                ]
            )
            + "\n"
        )
        assert actual_row == expected_row


def test_create_tsv_row_creates_audio_set(
    default_audio_args,
):
    audio_store = audio.AudioStore()
    audio_args = default_audio_args.copy()
    audio_set_data = {
        "audio_set": "test_audio_set",
        "set_url": "test.com",
        "set_position": 1,
        "set_thumbnail": "thumbnail.jpg",
    }
    audio_args["audio_set"] = audio_set_data
    test_audio = audio.Audio(**audio_args)

    def mock_url_validator(value):
        # This avoids needing the internet for testing.
        return value

    with patch.object(
        storage.columns.urls, "validate_url_string", side_effect=mock_url_validator
    ):
        actual_row = audio_store._create_tsv_row(test_audio)
        expected_row = (
            "\t".join(
                [
                    "foreign_id",
                    "https://landing_page.org",
                    "https://audiourl.org",
                    "https://thumbnail.com",
                    "\\N",
                    "by",
                    "4.0",
                    "tyler",
                    "https://creatorurl.com",
                    "agreatsong",
                    '{"description": "cat song"}',
                    '{"name": "tag1", "provider": "testing"}',
                    "\\N",
                    "testing_provider",
                    "testing_source",
                    "provider_api",
                    "100",
                    "\\N",
                    "\\N",
                    "music",
                    '{"rock", "pop"}',
                    '{"audio_set": "test_audio_set", "set_url": "test.com", '
                    '"set_position": "1", "set_thumbnail": "thumbnail.jpg"}',
                    "\\N",
                ]
            )
            + "\n"
        )
        assert actual_row == expected_row


def test_create_tsv_row_non_none_if_req_fields(
    default_audio_args,
):
    audio_store = audio.AudioStore()
    test_audio = audio.Audio(**default_audio_args)
    actual_row = audio_store._create_tsv_row(test_audio)
    assert actual_row is not None


def test_create_tsv_row_none_if_no_foreign_landing_url(
    default_audio_args,
):
    audio_store = audio.AudioStore()
    audio_args = default_audio_args
    audio_args["foreign_landing_url"] = None
    test_audio = audio.Audio(**audio_args)
    expect_row = None
    actual_row = audio_store._create_tsv_row(test_audio)
    assert expect_row == actual_row


def test_create_tsv_row_none_if_no_license(
    default_audio_args,
):
    audio_store = audio.AudioStore()
    audio_args = default_audio_args
    audio_args["license_"] = None
    test_audio = audio.Audio(**audio_args)
    expect_row = None
    actual_row = audio_store._create_tsv_row(test_audio)
    assert expect_row == actual_row


def test_create_tsv_row_none_if_no_license_version(
    default_audio_args,
):
    audio_store = audio.AudioStore()
    audio_args = default_audio_args
    audio_args["license_version"] = None
    test_audio = audio.Audio(**audio_args)
    expect_row = None
    actual_row = audio_store._create_tsv_row(test_audio)
    assert expect_row == actual_row


def test_create_tsv_row_returns_none_if_missing_audio_url(
    default_audio_args,
):
    audio_store = audio.AudioStore()
    audio_args = default_audio_args
    audio_args["url"] = None
    test_audio = audio.Audio(**audio_args)
    expect_row = None
    actual_row = audio_store._create_tsv_row(test_audio)
    assert expect_row == actual_row


def test_create_tsv_row_handles_empty_dict_and_tags(
    default_audio_args,
):
    audio_store = audio.AudioStore()
    meta_data = {}
    tags = []
    audio_args = default_audio_args
    audio_args["meta_data"] = meta_data
    audio_args["tags"] = tags
    test_audio = audio.Audio(**audio_args)

    actual_row = audio_store._create_tsv_row(test_audio).split("\t")
    actual_meta_data, actual_tags = None, None
    for i, field in enumerate(audio.Audio._fields):
        if field == "meta_data":
            actual_meta_data = actual_row[i]
        elif field == "tags":
            actual_tags = actual_row[i]
    assert actual_meta_data is not None and actual_tags is not None
    expect_meta_data, expect_tags = "\\N", "\\N"
    assert expect_meta_data == actual_meta_data
    assert expect_tags == actual_tags


def test_create_tsv_row_properly_places_entries(monkeypatch):
    def mock_validate_url(url_string):
        return url_string

    monkeypatch.setattr(storage.columns.urls, "validate_url_string", mock_validate_url)
    audio_store = audio.AudioStore()
    req_args_dict = {
        "foreign_landing_url": "https://landing_page.com",
        "url": "https://audiourl.com",
        "license_": "testlicense",
        "license_version": "1.0",
    }
    args_dict = {
        "foreign_identifier": "foreign_id",
        "thumbnail_url": "https://thumbnail.com",
        "duration": 200,
        "filesize": None,
        "creator": "tyler",
        "creator_url": "https://creatorurl.com",
        "title": "agreatsong",
        "meta_data": {"description": "a song about cat"},
        "tags": [{"name": "tag1", "provider": "testing"}],
        "watermarked": None,
        "bit_rate": 16000,
        "sample_rate": 44100,
        "category": "music",
        "genres": ["pop", "rock"],
        "audio_set": {
            "audio_set": "album",
            "set_position": 1,
            "set_url": "https://album.com/",
            "set_thumbnail": "https://album.com/thumbnail.jpg",
        },
        "alt_files": None,
        "provider": "testing_provider",
        "source": "testing_source",
        "ingestion_type": "provider_api",
    }
    args_dict.update(req_args_dict)

    test_audio = audio.Audio(**args_dict)
    actual_row = audio_store._create_tsv_row(test_audio)
    expect_row = (
        "\t".join(
            [
                "foreign_id",
                "https://landing_page.com",
                "https://audiourl.com",
                "https://thumbnail.com",
                "\\N",
                "testlicense",
                "1.0",
                "tyler",
                "https://creatorurl.com",
                "agreatsong",
                '{"description": "a song about cat"}',
                '[{"name": "tag1", "provider": "testing"}]',
                "\\N",
                "testing_provider",
                "testing_source",
                "provider_api",
                "200",
                "16000",
                "44100",
                "music",
                '{"pop", "rock"}',
                '{"audio_set": "album", "set_position": "1", "set_url": "https://album.com/", '
                '"set_thumbnail": "https://album.com/thumbnail.jpg"}',
                "\\N",
            ]
        )
        + "\n"
    )
    assert expect_row == actual_row

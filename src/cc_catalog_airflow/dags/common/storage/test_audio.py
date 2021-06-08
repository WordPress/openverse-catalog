import logging
import requests
import pytest
import tldextract

from common.licenses import licenses
from common.storage import audio
from common.storage import util

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.DEBUG)

logger = logging.getLogger(__name__)

# This avoids needing the internet for testing.
licenses.urls.tldextract.extract = tldextract.TLDExtract(
    suffix_list_urls=None
)
audio.columns.urls.tldextract.extract = tldextract.TLDExtract(
    suffix_list_urls=None
)

mock_audio_args = {
    'foreign_landing_url': 'https://landing_page.com',
    'audio_url': 'http://audiourl.com',
    'license_': 'by-nc',
    'license_version': '1.0',
    'license_url': None,
    'foreign_identifier': 'foreign_id',
    'thumbnail_url': 'http://thumbnail.com',
    'duration': 200,
    'creator': 'tyler',
    'creator_url': 'https://creatorurl.com',
    'title': 'agreatpicture',
    'meta_data': None,
    'raw_tags': None,
    'source': 'testing_source'
}


@pytest.fixture
def setup_env(monkeypatch):
    monkeypatch.setenv('OUTPUT_DIR', '/tmp')


@pytest.fixture
def mock_rewriter(monkeypatch):
    def mock_rewrite_redirected_url(url_string):
        return url_string

    monkeypatch.setattr(
        licenses.urls,
        'rewrite_redirected_url',
        mock_rewrite_redirected_url,
    )


@pytest.fixture
def get_good(monkeypatch):
    def mock_get(url, timeout=60):
        return requests.Response()

    monkeypatch.setattr(licenses.urls.requests, 'get', mock_get)


def test_AudioStore_uses_OUTPUT_DIR_variable(
        monkeypatch,
):
    testing_output_dir = '/my_output_dir'
    monkeypatch.setenv('OUTPUT_DIR', testing_output_dir)
    audio_store = audio.AudioStore()
    assert testing_output_dir in audio_store._OUTPUT_PATH


def test_AudioStore_falls_back_to_tmp_output_dir_variable(
        monkeypatch,
        setup_env,
):
    monkeypatch.delenv('OUTPUT_DIR')
    audio_store = audio.AudioStore()
    assert '/tmp' in audio_store._OUTPUT_PATH


def test_AudioStore_includes_provider_in_output_file_string(
        setup_env,
):
    audio_store = audio.AudioStore('test_provider')
    assert type(audio_store._OUTPUT_PATH) == str
    assert 'test_provider' in audio_store._OUTPUT_PATH


def test_AudioStore_add_item_adds_realistic_audio_to_buffer(
        setup_env, mock_rewriter
):
    license_url = 'https://creativecommons.org/publicdomain/zero/1.0/'
    audio_store = audio.AudioStore(provider='testing_provider')
    audio_store.add_item(
        foreign_landing_url='https://audios.org/audio01',
        audio_url='https://audios.org/audio01.jpg',
        license_url=license_url,
    )
    assert len(audio_store._media_buffer) == 1


def test_AudioStore_add_item_adds_multiple_audios_to_buffer(
        mock_rewriter, setup_env,
):
    audio_store = audio.AudioStore(provider='testing_provider')
    audio_store.add_item(
        foreign_landing_url='https://audios.org/audio01',
        audio_url='https://audios.org/audio01.jpg',
        license_url='https://creativecommons.org/publicdomain/zero/1.0/'
    )
    audio_store.add_item(
        foreign_landing_url='https://audios.org/audio02',
        audio_url='https://audios.org/audio02.jpg',
        license_url='https://creativecommons.org/publicdomain/zero/1.0/'
    )
    audio_store.add_item(
        foreign_landing_url='https://audios.org/audio03',
        audio_url='https://audios.org/audio03.jpg',
        license_url='https://creativecommons.org/publicdomain/zero/1.0/'
    )
    audio_store.add_item(
        foreign_landing_url='https://audios.org/audio04',
        audio_url='https://audios.org/audio04.jpg',
        license_url='https://creativecommons.org/publicdomain/zero/1.0/'
    )
    assert len(audio_store._media_buffer) == 4


def test_AudioStore_add_item_flushes_buffer(
        mock_rewriter, setup_env, tmpdir,
):
    output_file = 'testing.tsv'
    tmp_directory = tmpdir
    output_dir = str(tmp_directory)
    tmp_file = tmp_directory.join(output_file)
    tmp_path_full = str(tmp_file)

    audio_store = audio.AudioStore(
        provider='testing_provider',
        output_file=output_file,
        output_dir=output_dir,
        buffer_length=3
    )
    audio_store.add_item(
        foreign_landing_url='https://audios.org/audio01',
        audio_url='https://audios.org/audio01.jpg',
        license_url='https://creativecommons.org/publicdomain/zero/1.0/'
    )
    audio_store.add_item(
        foreign_landing_url='https://audios.org/audio02',
        audio_url='https://audios.org/audio02.jpg',
        license_url='https://creativecommons.org/publicdomain/zero/1.0/'
    )
    audio_store.add_item(
        foreign_landing_url='https://audios.org/audio03',
        audio_url='https://audios.org/audio03.jpg',
        license_url='https://creativecommons.org/publicdomain/zero/1.0/'
    )
    audio_store.add_item(
        foreign_landing_url='https://audios.org/audio04',
        audio_url='https://audios.org/audio04.jpg',
        license_url='https://creativecommons.org/publicdomain/zero/1.0/'
    )
    assert len(audio_store._media_buffer) == 1
    with open(tmp_path_full) as f:
        lines = f.read().split('\n')
    assert len(lines) == 4  # recall the last '\n' will create an empty line.


def test_AudioStore_commit_writes_nothing_if_no_lines_in_buffer():
    audio_store = audio.AudioStore(output_dir='/path/does/not/exist')
    audio_store.commit()


def test_AudioStore_produces_correct_total_audios(mock_rewriter, setup_env):
    audio_store = audio.AudioStore(provider='testing_provider')
    audio_store.add_item(
        foreign_landing_url='https://audios.org/audio01',
        audio_url='https://audios.org/audio01.jpg',
        license_url='https://creativecommons.org/publicdomain/zero/1.0/'
    )
    audio_store.add_item(
        foreign_landing_url='https://audios.org/audio02',
        audio_url='https://audios.org/audio02.jpg',
        license_url='https://creativecommons.org/publicdomain/zero/1.0/'
    )
    audio_store.add_item(
        foreign_landing_url='https://audios.org/audio03',
        audio_url='https://audios.org/audio03.jpg',
        license_url='https://creativecommons.org/publicdomain/zero/1.0/'
    )
    assert audio_store.total_items == 3


def test_AudioStore_get_audio_places_given_args(
        monkeypatch,
        setup_env
):
    audio_store = audio.AudioStore(provider='testing_provider')

    def mock_license_chooser(license_url, license_, license_version):
        return licenses.LicenseInfo(
            license_, license_version, license_url
        ), license_url

    monkeypatch.setattr(
        audio_store,
        'get_valid_license_info',
        mock_license_chooser
    )

    def mock_get_source(source, provider):
        return source

    monkeypatch.setattr(
        util,
        'get_source',
        mock_get_source
    )

    def mock_enrich_tags(tags):
        return tags

    monkeypatch.setattr(
        audio_store,
        '_enrich_tags',
        mock_enrich_tags
    )

    def mock_enrich_metadata(metadata, license_url=None, raw_license_url=None):
        return metadata

    monkeypatch.setattr(
        audio_store,
        '_enrich_meta_data',
        mock_enrich_metadata
    )
    args_dict = mock_audio_args.copy()
    actual_audio = audio_store._get_audio(**args_dict)
    args_dict['tags'] = args_dict.pop('raw_tags')
    args_dict.pop('license_url')
    args_dict['provider'] = 'testing_provider'
    args_dict['filesize'] = None
    assert actual_audio == audio.Audio(**args_dict)


def test_AudioStore_get_audio_calls_license_chooser(
        monkeypatch,
        setup_env,
):
    audio_store = audio.AudioStore()

    def mock_license_chooser(license_url, license_, license_version):
        return licenses.LicenseInfo(
            'diff_license', None, license_url
        ), license_url

    monkeypatch.setattr(
        audio_store,
        'get_valid_license_info',
        mock_license_chooser
    )
    audio_args = mock_audio_args.copy()
    # license_url = 'https://license/url',
    # license_ = 'license',
    # license_version = '1.5',

    actual_audio = audio_store._get_audio(
        **audio_args,
    )
    assert actual_audio.license_ == 'diff_license'


def test_AudioStore_returns_None_when_license_is_invalid(
        monkeypatch,
        setup_env,
):
    audio_store = audio.AudioStore()
    audio_args = mock_audio_args.copy()
    audio_args['license_url'] = 'https://license/url'
    audio_args['license_'] = 'license'
    audio_args['license_version'] = '1.5'

    actual_audio = audio_store._get_audio(
        **audio_args
    )
    assert actual_audio is None


def test_AudioStore_get_audio_gets_source(
        monkeypatch,
        setup_env,
):
    audio_store = audio.AudioStore()

    def mock_get_source(source, provider):
        return 'diff_source'

    monkeypatch.setattr(util, 'get_source', mock_get_source)

    actual_audio = audio_store._get_audio(
        **mock_audio_args
    )
    assert actual_audio.source == 'diff_source'


def test_AudioStore_get_audio_replaces_non_dict_meta_data_with_no_license_url(
        setup_env,
):
    audio_store = audio.AudioStore()

    audio_args = mock_audio_args.copy()
    audio_args['meta_data'] = 'notadict'
    audio_args['license_'] = 'by-nc-nd'
    audio_args['license_version'] = '4.0'
    audio_args['license_url'] = None
    actual_audio = audio_store._get_audio(
        **audio_args,
    )
    assert actual_audio.meta_data == {
        'license_url': 'https://creativecommons.org/licenses/by-nc-nd/4.0/', 'raw_license_url': None
    }


def test_AudioStore_get_audio_creates_meta_data_with_valid_license_url(
        monkeypatch, setup_env
):
    audio_store = audio.AudioStore()

    def mock_license_chooser(license_url, license_, license_version):
        return licenses.LicenseInfo(
            license_, license_version, license_url
        ), license_url

    monkeypatch.setattr(
        audio_store,
        'get_valid_license_info',
        mock_license_chooser
    )
    license_url = 'https://my.license.url'

    audio_args = mock_audio_args.copy()
    audio_args['meta_data'] = None
    audio_args['license_url'] = license_url
    actual_audio = audio_store._get_audio(
        **audio_args,
    )
    assert actual_audio.meta_data == {
        'license_url': license_url, 'raw_license_url': license_url
    }


def test_AudioStore_get_audio_adds_valid_license_url_to_dict_meta_data(
        monkeypatch, setup_env
):
    audio_store = audio.AudioStore()

    def mock_license_chooser(license_url, license_, license_version):
        return licenses.LicenseInfo(
            license_, license_version, license_url
        ), license_url

    monkeypatch.setattr(
        audio_store,
        'get_valid_license_info',
        mock_license_chooser
    )
    audio_args = mock_audio_args.copy()
    audio_args['meta_data'] = {'key1': 'val1'}
    audio_args['license_url'] = 'https://license/url'
    actual_audio = audio_store._get_audio(
        **audio_args
    )
    assert actual_audio.meta_data == {
        'key1': 'val1',
        'license_url': 'https://license/url',
        'raw_license_url': 'https://license/url'
    }


def test_AudioStore_get_audio_fixes_invalid_license_url(
        monkeypatch, setup_env
):
    audio_store = audio.AudioStore()

    original_url = 'https://license/url',
    updated_url = 'https://updatedurl.com'

    def mock_license_chooser(license_url, license_, license_version):
        return licenses.LicenseInfo(
            license_, license_version, updated_url
        ), license_url

    monkeypatch.setattr(
        audio_store,
        'get_valid_license_info',
        mock_license_chooser
    )

    audio_args = mock_audio_args.copy()
    audio_args['license_url'] = original_url
    audio_args['meta_data'] = None
    actual_audio = audio_store._get_audio(
        **audio_args,
    )
    assert actual_audio.meta_data == {
        'license_url': updated_url, 'raw_license_url': original_url
    }


def test_AudioStore_get_audio_enriches_singleton_tags(
        setup_env,
):
    audio_store = audio.AudioStore('test_provider')

    audio_args = mock_audio_args.copy()
    audio_args['raw_tags'] = ['lone']
    actual_audio = audio_store._get_audio(
        **audio_args,
    )

    assert actual_audio.tags == [{'name': 'lone', 'provider': 'test_provider'}]


def test_AudioStore_get_audio_tag_blacklist(
        setup_env,
):
    raw_tags = [
        'cc0',
        'valid',
        'garbage:=metacrap',
        'uploaded:by=flickrmobile',
        {
            'name': 'uploaded:by=instagram',
            'provider': 'test_provider'
        }
    ]

    audio_store = audio.AudioStore('test_provider')
    audio_args = mock_audio_args.copy()
    audio_args['raw_tags'] = raw_tags
    actual_audio = audio_store._get_audio(
        **audio_args,
    )

    assert actual_audio.tags == [
        {'name': 'valid', 'provider': 'test_provider'}
    ]


def test_AudioStore_get_audio_enriches_multiple_tags(
        setup_env,
):
    audio_store = audio.AudioStore('test_provider')
    audio_args = mock_audio_args.copy()
    audio_args['raw_tags'] = ['tagone', 'tag2', 'tag3']
    actual_audio = audio_store._get_audio(
        **audio_args,
    )

    assert actual_audio.tags == [
        {'name': 'tagone', 'provider': 'test_provider'},
        {'name': 'tag2', 'provider': 'test_provider'},
        {'name': 'tag3', 'provider': 'test_provider'},
    ]


def test_AudioStore_get_audio_leaves_preenriched_tags(
        setup_env
):
    audio_store = audio.AudioStore('test_provider')
    tags = [
        {'name': 'tagone', 'provider': 'test_provider'},
        {'name': 'tag2', 'provider': 'test_provider'},
        {'name': 'tag3', 'provider': 'test_provider'},
    ]
    audio_args = mock_audio_args.copy()
    audio_args['raw_tags'] = tags
    actual_audio = audio_store._get_audio(
        **audio_args,
    )

    assert actual_audio.tags == tags


def test_AudioStore_get_audio_nones_nonlist_tags(
        setup_env,
):
    audio_store = audio.AudioStore('test_provider')
    tags = 'notalist'
    audio_args = mock_audio_args.copy()
    audio_args['raw_tags'] = tags
    actual_audio = audio_store._get_audio(
        **audio_args,
    )

    assert actual_audio.tags is None


@pytest.fixture
def default_audio_args(
        setup_env,
):
    return dict(
        foreign_identifier=None,
        foreign_landing_url='https://audio.org',
        audio_url='https://audio.org',
        thumbnail_url=None,
        duration=0,
        filesize=None,
        license_='cc0',
        license_version='1.0',
        creator=None,
        creator_url=None,
        title=None,
        meta_data=None,
        tags=None,
        provider=None,
        source=None,
    )


def test_create_tsv_row_non_none_if_req_fields(
        default_audio_args,
        get_good,
        setup_env,
):
    audio_store = audio.AudioStore()
    test_audio = audio.Audio(**default_audio_args)
    actual_row = audio_store._create_tsv_row(test_audio)
    assert actual_row is not None


def test_create_tsv_row_none_if_no_foreign_landing_url(
        default_audio_args,
        setup_env,
):
    audio_store = audio.AudioStore()
    audio_args = default_audio_args
    audio_args['foreign_landing_url'] = None
    test_audio = audio.Audio(**audio_args)
    expect_row = None
    actual_row = audio_store._create_tsv_row(test_audio)
    assert expect_row == actual_row


def test_create_tsv_row_none_if_no_license(
        default_audio_args,
        setup_env,
):
    audio_store = audio.AudioStore()
    audio_args = default_audio_args
    audio_args['license_'] = None
    test_audio = audio.Audio(**audio_args)
    expect_row = None
    actual_row = audio_store._create_tsv_row(test_audio)
    assert expect_row == actual_row


def test_create_tsv_row_none_if_no_license_version(
        default_audio_args,
        setup_env,
):
    audio_store = audio.AudioStore()
    audio_args = default_audio_args
    audio_args['license_version'] = None
    test_audio = audio.Audio(**audio_args)
    expect_row = None
    actual_row = audio_store._create_tsv_row(test_audio)
    assert expect_row == actual_row


def test_create_tsv_row_returns_none_if_missing_audio_url(
        default_audio_args,
        setup_env,
):
    audio_store = audio.AudioStore()
    audio_args = default_audio_args
    audio_args['audio_url'] = None
    test_audio = audio.Audio(**audio_args)
    expect_row = None
    actual_row = audio_store._create_tsv_row(test_audio)
    assert expect_row == actual_row


def test_create_tsv_row_handles_empty_dict_and_tags(
        default_audio_args,
        setup_env,
):
    audio_store = audio.AudioStore()
    meta_data = {}
    tags = []
    audio_args = default_audio_args
    audio_args['meta_data'] = meta_data
    audio_args['tags'] = tags
    test_audio = audio.Audio(**audio_args)

    actual_row = audio_store._create_tsv_row(test_audio).split('\t')
    actual_meta_data, actual_tags = actual_row[12], actual_row[13]
    expect_meta_data, expect_tags = '\\N', '\\N'
    assert expect_meta_data == actual_meta_data
    assert expect_tags == actual_tags


def test_create_tsv_row_turns_empty_into_nullchar(
        default_audio_args,
        setup_env,
):
    audio_store = audio.AudioStore()
    audio_args = default_audio_args
    test_audio = audio.Audio(**audio_args)

    actual_row = audio_store._create_tsv_row(test_audio).split('\t')
    assert all(
        [
            actual_row[i] == '\\N'
            for i in [0, 3, 5, 8, 9, 10, 11, 12, 13]
        ]
    ) is True
    assert actual_row[-1] == '\\N\n'


def test_create_tsv_row_properly_places_entries(
        setup_env, monkeypatch
):
    def mock_validate_url(url_string):
        return url_string

    monkeypatch.setattr(
        audio.columns.urls, 'validate_url_string', mock_validate_url
    )
    audio_store = audio.AudioStore()
    req_args_dict = {
        'foreign_landing_url': 'https://landing_page.com',
        'audio_url': 'http://audiourl.com',
        'license_': 'testlicense',
        'license_version': '1.0',
    }
    args_dict = {
        'foreign_identifier': 'foreign_id',
        'thumbnail_url': 'http://thumbnail.com',
        'duration': 200,
        'filesize': None,
        'creator': 'tyler',
        'creator_url': 'https://creatorurl.com',
        'title': 'agreatpicture',
        'meta_data': {'description': 'cat picture'},
        'tags': [{'name': 'tag1', 'provider': 'testing'}],
        'provider': 'testing_provider',
        'source': 'testing_source'
    }
    args_dict.update(req_args_dict)

    test_audio = audio.Audio(**args_dict)
    actual_row = audio_store._create_tsv_row(
        test_audio
    )
    expect_row = '\t'.join([
        'foreign_id',
        'https://landing_page.com',
        'http://audiourl.com',
        'http://thumbnail.com',
        '200',
        '\\N',
        'testlicense',
        '1.0',
        'tyler',
        'https://creatorurl.com',
        'agreatpicture',
        '{"description": "cat picture"}',
        '[{"name": "tag1", "provider": "testing"}]',
        'testing_provider',
        'testing_source'
    ]) + '\n'
    assert expect_row == actual_row

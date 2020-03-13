import logging
import pytest

from common.storage import audio

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.DEBUG)


@pytest.fixture
def setup_env(monkeypatch):
    monkeypatch.setenv('OUTPUT_DIR', '/tmp')


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


def test_AudioStore_add_item_adds_realistic_image_to_buffer(
        setup_env,
):
    audio_store = audio.AudioStore(provider='testing_provider')
    audio_store.add_item(
        foreign_landing_url='https://images.org/image01',
        audio_url='https://images.org/image01.jpg',
        license_url='https://creativecommons.org/licenses/cc0/1.0/'
    )
    assert len(audio_store._image_buffer) == 1


def test_AudioStore_add_item_adds_multiple_images_to_buffer(
        setup_env,
):
    audio_store = audio.AudioStore(provider='testing_provider')
    audio_store.add_item(
        foreign_landing_url='https://images.org/image01',
        audio_url='https://images.org/image01.jpg',
        license_url='https://creativecommons.org/licenses/cc0/1.0/'
    )
    audio_store.add_item(
        foreign_landing_url='https://images.org/image02',
        audio_url='https://images.org/image02.jpg',
        license_url='https://creativecommons.org/licenses/cc0/1.0/'
    )
    audio_store.add_item(
        foreign_landing_url='https://images.org/image03',
        audio_url='https://images.org/image03.jpg',
        license_url='https://creativecommons.org/licenses/cc0/1.0/'
    )
    audio_store.add_item(
        foreign_landing_url='https://images.org/image04',
        audio_url='https://images.org/image04.jpg',
        license_url='https://creativecommons.org/licenses/cc0/1.0/'
    )
    assert len(audio_store._image_buffer) == 4


def test_AudioStore_add_item_flushes_buffer(
        tmpdir,
        setup_env,
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
        foreign_landing_url='https://images.org/image01',
        audio_url='https://images.org/image01.jpg',
        license_url='https://creativecommons.org/licenses/cc0/1.0/'
    )
    audio_store.add_item(
        foreign_landing_url='https://images.org/image02',
        audio_url='https://images.org/image02.jpg',
        license_url='https://creativecommons.org/licenses/cc0/1.0/'
    )
    audio_store.add_item(
        foreign_landing_url='https://images.org/image03',
        audio_url='https://images.org/image03.jpg',
        license_url='https://creativecommons.org/licenses/cc0/1.0/'
    )
    audio_store.add_item(
        foreign_landing_url='https://images.org/image04',
        audio_url='https://images.org/image04.jpg',
        license_url='https://creativecommons.org/licenses/cc0/1.0/'
    )
    assert len(audio_store._image_buffer) == 1
    with open(tmp_path_full) as f:
        lines = f.read().split('\n')
    assert len(lines) == 4  # recall the last '\n' will create an empty line.


def test_AudioStore_commit_writes_nothing_if_no_lines_in_buffer():
    audio_store = audio.AudioStore(output_dir='/path/does/not/exist')
    audio_store.commit()


def test_AudioStore_get_audio_places_given_args(
        monkeypatch,
        setup_env
):
    audio_store = audio.AudioStore(provider='testing_provider')
    args_dict = {
        'foreign_landing_url': 'https://landing_page.com',
        'audio_url': 'http://imageurl.com',
        'file_format': 'mp3',
        'duration': 42,
        'samplerate': 44100,
        'bitdepth': 8,
        'channels': 2,
        'license_': 'testlicense',
        'license_version': '1.0',
        'license_url': None,
        'foreign_identifier': 'foreign_id',
        'thumbnail_url': 'http://thumbnail.com',
        'creator': 'tyler',
        'creator_url': 'https://creatorurl.com',
        'title': 'agreatpicture',
        'album': 'analbum',
        'genre': 'nature',
        'language': 'English',
        'meta_data': {'description': 'cat meow'},
        'raw_tags': [{'name': 'tag1', 'provider': 'testing'}],
        'source': 'testing_source'
    }

    def mock_license_chooser(license_url, license_, license_version):
        return license_, license_version
    monkeypatch.setattr(
        audio.util,
        'choose_license_and_version',
        mock_license_chooser
    )

    def mock_get_source(source, provider):
        return source
    monkeypatch.setattr(
        audio.util,
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

    actual_audio = audio_store._get_audio(**args_dict)
    args_dict['tags'] = args_dict.pop('raw_tags')
    args_dict.pop('license_url')
    args_dict['provider'] = 'testing_provider'
    args_dict['filesize'] = None
    assert actual_audio == audio._Audio(**args_dict)


def test_AudioStore_get_audio_calls_license_chooser(
        monkeypatch,
        setup_env,
):
    audio_store = audio.AudioStore()

    def mock_license_chooser(license_url, license_, license_version):
        return 'diff_license', None
    monkeypatch.setattr(
        audio.util,
        'choose_license_and_version',
        mock_license_chooser
    )

    actual_audio = audio_store._get_audio(
        license_url='https://license/url',
        license_='license',
        license_version='1.5',
        foreign_landing_url=None,
        audio_url=None,
        file_format=None,
        duration=None,
        samplerate=None,
        bitdepth=None,
        channels=None,
        thumbnail_url=None,
        foreign_identifier=None,
        creator=None,
        creator_url=None,
        title=None,
        album=None,
        genre=None,
        language=None,
        meta_data=None,
        raw_tags=None,
        source=None,
    )
    assert actual_audio.license_ == 'diff_license'


def test_AudioStore_get_audio_gets_source(
        monkeypatch,
        setup_env,
):
    audio_store = audio.AudioStore()

    def mock_get_source(source, provider):
        return 'diff_source'
    monkeypatch.setattr(audio.util, 'get_source', mock_get_source)

    actual_audio = audio_store._get_audio(
        license_url='https://license/url',
        license_='license',
        license_version='1.5',
        foreign_landing_url=None,
        audio_url=None,
        file_format=None,
        duration=None,
        samplerate=None,
        bitdepth=None,
        channels=None,
        thumbnail_url=None,
        foreign_identifier=None,
        creator=None,
        creator_url=None,
        title=None,
        album=None,
        genre=None,
        language=None,
        meta_data=None,
        raw_tags=None,
        source=None,
    )
    assert actual_audio.source == 'diff_source'


def test_AudioStore_get_audio_replaces_non_dict_meta_data_with_no_license_url(
        setup_env,
):
    audio_store = audio.AudioStore()

    actual_audio = audio_store._get_audio(
        license_url=None,
        license_='license',
        license_version='1.5',
        foreign_landing_url=None,
        audio_url=None,
        file_format=None,
        duration=None,
        samplerate=None,
        bitdepth=None,
        channels=None,
        thumbnail_url=None,
        foreign_identifier=None,
        creator=None,
        creator_url=None,
        title=None,
        album=None,
        genre=None,
        language=None,
        meta_data='notadict',
        raw_tags=None,
        source=None,
    )
    assert actual_audio.meta_data == {'license_url': None}


def test_AudioStore_get_audio_creates_meta_data_with_license_url(
        setup_env,
):
    license_url = 'https://my.license.url'
    audio_store = audio.AudioStore()

    actual_audio = audio_store._get_audio(
        license_url=license_url,
        license_='license',
        license_version='1.5',
        foreign_landing_url=None,
        audio_url=None,
        file_format=None,
        duration=None,
        samplerate=None,
        bitdepth=None,
        channels=None,
        thumbnail_url=None,
        foreign_identifier=None,
        creator=None,
        creator_url=None,
        title=None,
        album=None,
        genre=None,
        language=None,
        meta_data=None,
        raw_tags=None,
        source=None,
    )
    assert actual_audio.meta_data == {'license_url': license_url}


def test_AudioStore_get_audio_adds_license_url_to_dict_meta_data(
        setup_env,
):
    audio_store = audio.AudioStore()

    actual_audio = audio_store._get_audio(
        license_url='https://license/url',
        license_='license',
        license_version='1.5',
        foreign_landing_url=None,
        audio_url=None,
        file_format=None,
        duration=None,
        samplerate=None,
        bitdepth=None,
        channels=None,
        thumbnail_url=None,
        foreign_identifier=None,
        creator=None,
        creator_url=None,
        title=None,
        album=None,
        genre=None,
        language=None,
        meta_data={'key1': 'val1'},
        raw_tags=None,
        source=None,
    )
    assert actual_audio.meta_data == {
        'key1': 'val1',
        'license_url': 'https://license/url'
    }


def test_AudioStore_get_audio_enriches_singleton_tags(
        setup_env,
):
    audio_store = audio.AudioStore('test_provider')

    actual_audio = audio_store._get_audio(
        license_url='https://license/url',
        license_='license',
        license_version='1.5',
        foreign_landing_url=None,
        audio_url=None,
        file_format=None,
        duration=None,
        samplerate=None,
        bitdepth=None,
        channels=None,
        thumbnail_url=None,
        foreign_identifier=None,
        creator=None,
        creator_url=None,
        title=None,
        album=None,
        genre=None,
        language=None,
        meta_data=None,
        raw_tags=['lone'],
        source=None,
    )

    assert actual_audio.tags == [{'name': 'lone', 'provider': 'test_provider'}]


def test_AudioStore_get_audio_enriches_multiple_tags(
        setup_env,
):
    audio_store = audio.AudioStore('test_provider')
    actual_audio = audio_store._get_audio(
        license_url='https://license/url',
        license_='license',
        license_version='1.5',
        foreign_landing_url=None,
        audio_url=None,
        file_format=None,
        duration=None,
        samplerate=None,
        bitdepth=None,
        channels=None,
        thumbnail_url=None,
        foreign_identifier=None,
        creator=None,
        creator_url=None,
        title=None,
        album=None,
        genre=None,
        language=None,
        meta_data=None,
        raw_tags=['tagone', 'tag2', 'tag3'],
        source=None,
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

    actual_audio = audio_store._get_audio(
        license_url='https://license/url',
        license_='license',
        license_version='1.5',
        foreign_landing_url=None,
        audio_url=None,
        file_format=None,
        duration=None,
        samplerate=None,
        bitdepth=None,
        channels=None,
        thumbnail_url=None,
        foreign_identifier=None,
        creator=None,
        creator_url=None,
        title=None,
        album=None,
        genre=None,
        language=None,
        meta_data=None,
        raw_tags=tags,
        source=None,
    )

    assert actual_audio.tags == tags


def test_AudioStore_get_audio_nones_nonlist_tags(
        setup_env,
):
    audio_store = audio.AudioStore('test_provider')
    tags = 'notalist'

    actual_audio = audio_store._get_audio(
        license_url='https://license/url',
        license_='license',
        license_version='1.5',
        foreign_landing_url=None,
        audio_url=None,
        file_format=None,
        duration=None,
        samplerate=None,
        bitdepth=None,
        channels=None,
        thumbnail_url=None,
        foreign_identifier=None,
        creator=None,
        creator_url=None,
        title=None,
        album=None,
        genre=None,
        language=None,
        meta_data=None,
        raw_tags=tags,
        source=None,
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
        file_format=None,
        duration=None,
        samplerate=None,
        bitdepth=None,
        channels=None,
        thumbnail_url=None,
        filesize=None,
        license_='cc0',
        license_version='1.0',
        creator=None,
        creator_url=None,
        title=None,
        album=None,
        genre=None,
        language=None,
        meta_data=None,
        tags=None,
        provider=None,
        source=None,
    )


def test_create_tsv_row_non_none_if_req_fields(
        default_audio_args,
        setup_env,
):
    audio_store = audio.AudioStore()
    test_audio = audio._Audio(**default_audio_args)
    actual_row = audio_store._create_tsv_row(test_audio)
    assert actual_row is not None


def test_create_tsv_row_none_if_no_foreign_landing_url(
        default_audio_args,
        setup_env,
):
    audio_store = audio.AudioStore()
    audio_args = default_audio_args
    audio_args['foreign_landing_url'] = None
    test_audio = audio._Audio(**audio_args)
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
    test_audio = audio._Audio(**audio_args)
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
    test_audio = audio._Audio(**audio_args)
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
    test_audio = audio._Audio(**audio_args)
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
    test_audio = audio._Audio(**audio_args)

    actual_row = audio_store._create_tsv_row(test_audio).split('\t')
    actual_meta_data, actual_tags = actual_row[18], actual_row[19]
    expect_meta_data, expect_tags = '\\N', '\\N'
    assert expect_meta_data == actual_meta_data
    assert expect_tags == actual_tags


def test_create_tsv_row_turns_empty_into_nullchar(
        default_audio_args,
        setup_env,
):
    audio_store = audio.AudioStore()
    audio_args = default_audio_args
    test_audio = audio._Audio(**audio_args)

    actual_row = audio_store._create_tsv_row(test_audio).split('\t')
    assert all(
        [
            actual_row[i] == '\\N'
            for i in [0, 3, 4, 5, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 18, 19, 20]
        ]
    ) is True
    assert actual_row[-1] == '\\N\n'


def test_create_tsv_row_properly_places_entries(
        setup_env,
):
    audio_store = audio.AudioStore()
    req_args_dict = {
        'foreign_landing_url': 'https://landing_page.com',
        'audio_url': 'http://imageurl.com',
        'license_': 'testlicense',
        'license_version': '1.0',
    }
    args_dict = {
        'foreign_identifier': 'foreign_id',
        'thumbnail_url': 'http://thumbnail.com',
        'file_format': 'mp3',
        'filesize': None,
        'duration': 42,
        'samplerate': 44100,
        'bitdepth': 8,
        'channels': 2,
        'creator': 'tyler',
        'creator_url': 'https://creatorurl.com',
        'title': 'agreatsound',
        'album': 'analbum',
        'genre': 'nature',
        'language': 'English',
        'meta_data': {'description': 'cat meow'},
        'tags': [{'name': 'tag1', 'provider': 'testing'}],
        'provider': 'testing_provider',
        'source': 'testing_source'
    }
    args_dict.update(req_args_dict)

    test_audio = audio._Audio(**args_dict)
    actual_row = audio_store._create_tsv_row(
        test_audio
    )
    expect_row = '\t'.join([
        'foreign_id',
        'https://landing_page.com',
        'http://imageurl.com',
        'mp3',
        'http://thumbnail.com',
        '\\N',
        '42',
        '44100',
        '8',
        '2',
        'testlicense',
        '1.0',
        'tyler',
        'https://creatorurl.com',
        'agreatsound',
        'analbum',
        'nature',
        'English',
        '{"description": "cat meow"}',
        '[{"name": "tag1", "provider": "testing"}]',
        'testing_provider',
        'testing_source'
    ]) + '\n'
    assert expect_row == actual_row

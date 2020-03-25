import json
import logging
import os
import requests
import pytest
from unittest.mock import patch, MagicMock

import internet_archive as ia

RESOURCES = os.path.join(
    os.path.abspath(os.path.dirname(__file__)
                    ), 'tests/resources/internet_archive'
)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.DEBUG,
)


def test_derive_timestamp_pair():
    actual_start_ts, actual_end_ts = ia._derive_timestamp_pair('2020-03-17')
    assert actual_start_ts == '2020-03-17'
    assert actual_end_ts == '2020-03-18'


def test_get_total_pages():
    r = requests.Response()
    r.status_code = 200
    with open(
            os.path.join(RESOURCES, 'total_pages_response.json')
    ) as f:
        response_dict = json.load(f)
    r.json = MagicMock(return_value=response_dict)
    start_timestamp = '2019-03-18'
    end_timestamp = '2019-03-19'
    expected_result = 5
    with patch.object(
            ia.delayed_requester,
            'get',
            return_value=r
    ) as mock_total_pages:
        mock_total_pages.result = ia._get_total_pages(start_timestamp, end_timestamp)

    assert mock_total_pages.result == expected_result


def test_build_query_params_adds_date_and_page():
    start_date = '2019-03-18'
    end_date = '2019-03-19'
    page = 2
    result_params = ia._build_query_params(
        start_date, end_date, page
    )
    assert result_params['q'] == "mediatype:audio AND date:[2019-03-18 TO 2019-03-19]"
    assert result_params['page'] == 2


def test_get_sound_for_page():
    r = requests.Response()
    r.status_code = 200
    start_timestamp = '2019-03-18'
    end_timestamp = '2019-03-19'
    with open(
            os.path.join(RESOURCES, 'total_pages_response.json')
    ) as f:
        response_dict = json.load(f)

    r.json = MagicMock(return_value=response_dict)
    expected_result = [
        {
            "collection": [
                "audio",
                "folksoundomy"
            ],
            "identifier": "0--Twitt-alhlhl91192",
            "mediatype": "audio",
            "title": "0--Twitt-alhlhl91192"
        },
        {
            "collection": [
                "audio",
                "folksoundomy"
            ],
            "identifier": "0-01-Titel",
            "mediatype": "audio",
            "title": "0-01-Titel"
        }
    ]
    with patch.object(
            ia.delayed_requester,
            'get',
            return_value=r
    ) as mock_get_sounds_for_page:
        mock_get_sounds_for_page.actual_result = ia._get_sound_for_page(start_timestamp, end_timestamp, 1)

    assert expected_result == mock_get_sounds_for_page.actual_result


def test_get_meta_data_and_download_url():
    identifier = '01MademoiselleFifi'
    r = requests.Response()
    r.status_code = 200
    with open(
            os.path.join(RESOURCES, 'expected_metadata.json')
    ) as f:
        expected_meta = json.load(f)

    with open(
            os.path.join(RESOURCES, 'metadata.json')
    ) as f:
        response_dict = json.load(f)
    r.json = MagicMock(return_value=response_dict)

    with patch.object(
            ia.delayed_requester,
            'get',
            return_value=r
    ) as mock_meta:
        mock_meta.actual_meta = ia._get_meta_data_and_download_url(identifier=identifier)["meta_data"]
        mock_meta.actual_url = ia._get_meta_data_and_download_url(identifier=identifier)["download_url"]

    expected_url = "https://archive.org/download/01MademoiselleFifi/01" \
                   " Mademoiselle Fifi.mp3"
    assert expected_meta == mock_meta.actual_meta
    assert expected_url == mock_meta.actual_url


def test_get_response_json_retries_with_none_response():
    with patch.object(
            ia.delayed_requester,
            'get',
            return_value=None
    ) as mock_get:
        with pytest.raises(Exception):
            assert ia._get_response_json({}, retries=2)

    assert mock_get.call_count == 3


def test_get_response_json_retries_with_non_ok():
    r = requests.Response()
    r.status_code = 504
    with open(
            os.path.join(RESOURCES, 'get_json_response.json')
    ) as f:
        response = json.load(f)
    r.json = MagicMock(return_value=response)
    with patch.object(
            ia.delayed_requester,
            'get',
            return_value=r
    ) as mock_get:
        with pytest.raises(Exception):
            assert ia._get_response_json({}, retries=2)

    assert mock_get.call_count == 3


def test_get_response_json_retries_with_error_json():
    r = requests.Response()
    r.status_code = 200
    r.json = MagicMock(return_value={'error': ''})
    with patch.object(
            ia.delayed_requester,
            'get',
            return_value=r
    ) as mock_get:
        with pytest.raises(Exception):
            assert ia._get_response_json({}, retries=2)

    assert mock_get.call_count == 3


def test_get_response_json_returns_response_json_when_all_ok():
    r = requests.Response()
    r.status_code = 200
    with open(
            os.path.join(RESOURCES, 'get_json_response.json')
    ) as f:
        response = json.load(f)
    r.json = MagicMock(return_value=response)
    with patch.object(
            ia.delayed_requester,
            'get',
            return_value=r
    ) as mock_get:
        actual_response_json = ia._get_response_json({}, retries=2)

    assert mock_get.call_count == 1
    assert actual_response_json == response

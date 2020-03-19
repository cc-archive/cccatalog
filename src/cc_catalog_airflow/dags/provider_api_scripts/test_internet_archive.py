import json
import logging
import os
import pickle
import requests
from unittest.mock import patch, MagicMock

import pytest

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
    start_timestamp = '2019-03-18'
    end_timestamp = '2019-03-19'
    expected_result = 5
    result = ia._get_total_pages(start_timestamp, end_timestamp)
    assert expected_result == result


def test_build_query_params_adds_date_and_page():
    start_date = '2019-03-18'
    end_date = '2019-03-19'
    page = 2
    result_params = ia._build_query_params(
        start_date, end_date, page
    )
    assert result_params['q'] == "mediatype:audio AND" + \
        " date:[2019-03-18 TO 2019-03-19]"
    assert result_params['page'] == 2


def test_get_sound_for_page():
    start_timestamp = '2019-03-18'
    end_timestamp = '2019-03-19'
    ia.DEFAULT_QUERY_PARAMS['rows'] = '2'
    expected_result = [{'collection': ['opensource_audio',
                                       'fav-david_smith1967'],
                        'date': '2019-03-18T00:00:00Z',
                        'identifier': '01MademoiselleFifi',
                        'licenseurl': 'http://creativecommons.org/' +
                        'publicdomain/mark/1.0/',
                        'mediatype': 'audio',
                        'title': 'Family Theater: Set 10'},
                       {'collection': ['opensource_audio'],
                        'date': '2019-03-19T00:00:00Z',
                        'identifier': '02IHaveFaith',
                        'mediatype': 'audio',
                        'title': 'John Wayne: 45: Walk With Him'}]

    actual_result = ia._get_sound_for_page(start_timestamp, end_timestamp, 1)

    assert expected_result == actual_result


def test_get_meta_data():
    identifier = '01MademoiselleFifi'

    with open(
            os.path.join(RESOURCES, 'expected_metadata.json')
    ) as f:
        expected_meta = json.load(f)

    actual_meta = ia._get_meta_data(identifier)

    assert expected_meta == actual_meta


def test_get_download_url():
    identifier = '01MademoiselleFifi'
    expected_url = "https://archive.org/download/01MademoiselleFifi/01"+
    " Mademoiselle Fifi.mp3"

    actual_url = ia._get_download_url(identifier)

    assert actual_url == expected_url


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
    response = {'responseHeader': {'status': 0,
                                   'QTime': 254,
                                   'params': {'query': 'mediatype:audio' +
                                              ' AND date:[2019-03-' +
                                              '18T00\\:00\\:00Z TO ' +
                                              '2019-03-18T23\\:59\\:59Z]',
                                              'qin': 'mediatype:audio ' +
                                              'AND date:[2019-03-18 TO' +
                                              ' 2019-03-18]',
                                              'fields': 'identifier,title' +
                                              ',mediatype,collection,' +
                                              'licenseurl,date',
                                              'wt': 'json',
                                              'rows': '200',
                                              'start': 0}},
                'response': {'numFound': 0,
                             'start': 0,
                             'docs': []}}
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
    response = {'responseHeader': {'status': 0,
                                   'QTime': 254,
                                   'params': {'query': 'mediatype:audio' +
                                              ' AND date:[2019-03-' +
                                              '18T00\\:00\\:00Z TO ' +
                                              '2019-03-18T23\\:59\\:59Z]',
                                              'qin': 'mediatype:audio ' +
                                              'AND date:[2019-03-18 TO' +
                                              ' 2019-03-18]',
                                              'fields': 'identifier,title' +
                                              ',mediatype,collection,' +
                                              'licenseurl,date',
                                              'wt': 'json',
                                              'rows': '200',
                                              'start': 0}},
                'response': {'numFound': 0,
                             'start': 0,
                             'docs': []}}
    r = requests.Response()
    r.status_code = 200
    r.json = MagicMock(return_value=response)
    with patch.object(
            ia.delayed_requester,
            'get',
            return_value=r
    ) as mock_get:
        actual_response_json = ia._get_response_json({}, retries=2)

    assert mock_get.call_count == 1
    assert actual_response_json == response

import logging
import os
import pytest
import requests
from unittest.mock import patch, MagicMock

import thingiverse as tiv

RESOURCES = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'tests/resources/thingiverse'
)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.DEBUG,
)


def test_derive_timestamp_pair():
    # Note that the timestamps are derived as if input was in UTC.
    start_ts, end_ts = tiv._derive_timestamp_pair('2018-01-15')
    assert start_ts == '1515974400'
    assert end_ts == '1516060800'


def test_get_response_json_retries_with_none_response():
    with patch.object(
            tiv.delayed_requester,
            'get',
            return_value=None
    ) as mock_get:
        with pytest.raises(Exception):
            assert tiv._get_response_json({}, retries=2)

    assert mock_get.call_count == 3


def test_get_response_json_retries_with_non_ok():
    r = requests.Response()
    r.status_code = 504
    r.json = MagicMock(return_value={'batchcomplete': ''})
    with patch.object(
            tiv.delayed_requester,
            'get',
            return_value=r
    ) as mock_get:
        with pytest.raises(Exception):
            assert tiv._get_response_json({}, retries=2)

    assert mock_get.call_count == 3


def test_get_response_json_retries_with_error_json():
    r = requests.Response()
    r.status_code = 200
    r.json = MagicMock(return_value={'error': ''})
    with patch.object(
            tiv.delayed_requester,
            'get',
            return_value=r
    ) as mock_get:
        with pytest.raises(Exception):
            assert tiv._get_response_json({}, retries=2)

    assert mock_get.call_count == 3


def test_get_response_json_returns_response_json_when_all_ok():
    expect_response_json = {'batchcomplete': ''}
    r = requests.Response()
    r.status_code = 200
    r.json = MagicMock(return_value=expect_response_json)
    with patch.object(
            tiv.delayed_requester,
            'get',
            return_value=r
    ) as mock_get:
        actual_response_json = tiv._get_response_json({}, retries=2)

    assert mock_get.call_count == 1
    assert actual_response_json == expect_response_json


def test_build_query_params_default():
    actual_query_params = tiv._build_query_params()
    expect_query_params = {
        'access_token': '32044862bce84c5399505e5c85d40c2a',
        'per_page': 30,
        'page': 1
    }
    assert actual_query_params == expect_query_params


def test_build_query_params_with_givens():
    actual_query_params = tiv._build_query_params(page=3)
    expect_query_params = {
        'access_token': '32044862bce84c5399505e5c85d40c2a',
        'per_page': 30,
        'page': 3
    }
    assert actual_query_params == expect_query_params


def test_build_thing_query_with_givens():
    actual_endpoint, actual_query_params = tiv._build_thing_query(5)
    expect_query_params = {
        'access_token': '32044862bce84c5399505e5c85d40c2a',
    }
    expected_endpoint = 'https://api.thingiverse.com/things/5'
    assert actual_query_params == expect_query_params and actual_endpoint == expected_endpoint

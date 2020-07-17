import os
import json
import logging
import requests
from unittest.mock import MagicMock, patch

import iconfinder as icf

RESOURCES = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'tests/resources/iconfinder'
)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.DEBUG
)


def _get_resource_json(json_name):
    with open(os.path.join(RESOURCES, json_name)) as f:
        resource_json = json.load(f)
    return resource_json


def test_get_query_param_default():
    actual_query_param = icf._get_query_param()
    expected_query_param = {
        "query": ":",
        "count": 100,
        "offset": 0,
        "premium": 0
    }

    assert actual_query_param == expected_query_param


def test_get_query_param_offset():
    actual_query_param = icf._get_query_param(
        offset=100
    )
    expected_query_param = {
        "query": ":",
        "count": 100,
        "offset": 100,
        "premium": 0
    }

    assert actual_query_param == expected_query_param


def test_request_handler_search():
    query_param = {
        "query": ":",
        "count": 10,
        "offset": 0,
        "premium": 0
    }
    search_response = _get_resource_json("search_response.json")
    r = requests.Response()
    r.status_code = 200
    r.json = MagicMock(return_value=search_response)

    with patch.object(
            icf.delay_request,
            'get',
            return_value=r) as mock_call:
        actual_response = icf._request_handler(
            params=query_param
        )

    assert mock_call.call_count == 1
    assert actual_response == search_response


def test_request_handler_icon():
    icon_id = 194929
    icon_response = _get_resource_json("icon_response.json")
    r = requests.Response()
    r.status_code = 200
    r.json = MagicMock(return_value=icon_response)

    with patch.object(
            icf.delay_request,
            'get',
            return_value=r) as mock_call:
        actual_response = icf._request_handler(
            endpoint=icf.SEARCH_ENDPOINT + str(icon_id)
        )

    assert mock_call.call_count == 1
    assert actual_response == icon_response


def test_request_handler_error():
    query_param = {
        "query": ":",
        "count": 10,
        "offset": 0,
        "premium": 0
    }
    r = requests.Response()
    r.status_code = 400

    with patch.object(
            icf.delay_request,
            'get',
            return_value=r) as mock_call:
        actual_response = icf._request_handler(
            params=query_param
        )

    assert mock_call.call_count == 3
    assert actual_response is None


# def test_process_icon_batch():
#     icon_batch = _get_resource_json("icon_batch.json")
#     icon_detail = _get_resource_json("icon_response.json")

#     r = requests.Response()
#     r.status_code = 200
#     r.json = MagicMock(return_value=icon_detail)

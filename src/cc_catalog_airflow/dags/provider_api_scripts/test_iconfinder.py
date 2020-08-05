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


def test_process_icon_batch_success():
    icon_batch = _get_resource_json("icon_batch.json")
    icon_detail = _get_resource_json("icon_response.json")

    r = requests.Response()
    r.status_code = 200
    r.json = MagicMock(return_value=icon_detail)

    with patch.object(
            icf.delay_request,
            'get',
            return_value=r) as mock_call:
        with patch.object(
                icf.image_store,
                'add_item') as mock_item:
            icf._process_icon_batch(icon_batch)

    assert mock_item.call_count == 1


def test_process_icon_batch_failure():
    icon_batch = _get_resource_json("icon_batch.json")

    r = requests.Response()
    r.status_code = 400

    with patch.object(
            icf.delay_request,
            'get',
            return_value=r) as mock_call:
        with patch.object(
                icf.image_store,
                'add_item') as mock_item:
            icf._process_icon_batch(icon_batch)

    assert mock_item.call_count == 0


def test_get_license_success():
    icon_detail = _get_resource_json("icon_response.json")
    actual_license_url = icf._get_license(
        icon_detail.get("iconset", {}).get("license")
    )

    assert actual_license_url == "http://creativecommons.org/licenses/by/3.0/"


def test_get_license_failure():
    license_ = {}
    actual_license_url = icf._get_license(license_)

    assert actual_license_url is None


def test_get_images_success():
    icon_detail = _get_resource_json("icon_response.json")

    actual_image_url, actual_thumbnail,\
        actual_height, actual_width = icf._get_images(
            icon_detail.get("raster_sizes")
        )

    assert actual_image_url == "https://cdn3.iconfinder.com/data/icons/peelicons-vol-1/50/Facebook-512.png"
    assert actual_thumbnail == "https://cdn3.iconfinder.com/data/icons/peelicons-vol-1/50/Facebook-512.png"
    assert actual_height == 512
    assert actual_width == 512


def test_get_image_failure():
    raster_sizes = []

    actual_image_url, actual_thumbnail,\
        actual_height, actual_width = icf._get_images(raster_sizes)

    assert actual_image_url is None
    assert actual_thumbnail is None
    assert actual_height is None
    assert actual_width is None


def test_get_creator_success():
    icon_detail = _get_resource_json("icon_response.json")

    actual_creator = icf._get_creator(
        icon_detail.get("iconset", {}).get("author")
    )

    assert actual_creator == "Neil Hainsworth"


def test_get_creator_failure():
    author = {}

    actual_creator = icf._get_creator(author)
    assert actual_creator is None


def test_get_metadata():
    icon_data = _get_resource_json("icon_response.json")
    expected_metadata = _get_resource_json("metadata.json")

    actual_metadata = icf._get_metadata(
        icon_data
    )

    assert actual_metadata == expected_metadata

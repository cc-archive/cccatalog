import os
import json
import logging
import requests
from unittest.mock import patch, MagicMock

import brooklyn_museum as bkm

RESOURCES = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'tests/resources/brooklynmuseum'
)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.DEBUG
)


def _get_resource_json(json_name):
    with open(os.path.join(RESOURCES, json_name)) as f:
        resource_json = json.load(f)
    return resource_json


def test_build_query_param_default():
    actual_param = bkm._get_query_param()
    expected_param = {
        "has_images": 1,
        "rights_type_permissive": 1,
        "limit": 35,
        "offset": 0
    }
    assert actual_param == expected_param


def test_build_query_param_given():
    actual_param = bkm._get_query_param(offset=35)
    expected_param = {
        "has_images": 1,
        "rights_type_permissive": 1,
        "limit": 35,
        "offset": 35
    }
    assert actual_param == expected_param


def test_get_response_failure():
    param = {
        "has_images": 1,
        "rights_type_permissive": 1,
        "limit": -1,
        "offset": 0
    }
    response_json = _get_resource_json("response_error.json")
    r = requests.Response()
    r.status_code = 500
    r.json = MagicMock(return_value=response_json)
    with patch.object(bkm.delay_request,
                      'get',
                      return_value=r) as mock_get:

        actual_data = bkm._get_response(query_param=param)
    expected_data = None

    assert mock_get.call_count == 3
    assert actual_data == expected_data


def test_get_response_success():
    param = {
        "has_images": 1,
        "rights_type_permissive": 1,
        "limit": 1,
        "offset": 0
    }
    response_json = _get_resource_json("response_success.json")
    r = requests.Response()
    r.status_code = 200
    r.json = MagicMock(return_value=response_json)
    with patch.object(bkm.delay_request,
                      'get',
                      return_value=r) as mock_get:

        actual_data = bkm._get_response(query_param=param)
    expected_data = response_json["data"]
    assert mock_get.call_count == 1
    assert actual_data == expected_data


def test_get_response_nodata():
    param = {
        "has_images": 1,
        "rights_type_permissive": 1,
        "limit": 1,
        "offset": 70000
    }
    response_json = _get_resource_json("response_nodata.json")
    r = requests.Response()
    r.status_code = 200
    r.json = MagicMock(return_value=response_json)
    with patch.object(bkm.delay_request,
                      'get',
                      return_value=r) as mock_get:

        actual_data = bkm._get_response(query_param=param)

    assert len(actual_data) == 0
    assert mock_get.call_count == 1


def test_object_response_success():
    response_json = _get_resource_json("complete_data.json")
    r = requests.Response()
    r.status_code = 200
    r.json = MagicMock(return_value=response_json)
    with patch.object(bkm.delay_request,
                      'get',
                      return_value=r) as mock_get:

        actual_data = bkm._get_response(bkm.ENDPOINT+str(1))
    expected_data = response_json["data"]

    assert mock_get.call_count == 1
    assert actual_data == expected_data


def test_handle_response():
    response_json = _get_resource_json("object_data.json")
    actual_image_count = bkm._handle_response(response_json)
    expected_image_count = 1

    assert actual_image_count == expected_image_count


def test_handle_response_nodata():
    response_json = None
    actual_image_count = bkm._handle_response(response_json)
    expected_image_count = 0

    assert actual_image_count == expected_image_count


def test_get_image_size():
    response_json = _get_resource_json("image_details.json")
    actual_height, actual_width = bkm._get_image_sizes(response_json)
    expected_height, expected_width = (1152, 1536)

    assert actual_height == expected_height
    assert actual_width == expected_width


def test_get_image_no_size():
    response_json = _get_resource_json("image_nosize.json")
    actual_height, actual_width = bkm._get_image_sizes(response_json)
    expected_height, expected_width = (None, None)

    assert actual_height == expected_height
    assert actual_width == expected_width


def test_get_license_url():
    response_json = _get_resource_json("license_info.json")
    actual_url = bkm._get_license_url(response_json)
    expected_url = 'https://creativecommons.org/licenses/by/3.0/"'

    assert actual_url == expected_url


def test_get_no_license_url():
    data = {
        "name": "Public Domain"
    }
    actual_url = bkm._get_license_url(data)
    expected_url = None

    assert actual_url == expected_url


def test_get_metadata():
    response_json = _get_resource_json("object_data.json")
    actual_metadata = bkm._get_metadata(response_json)
    expected_metadata = _get_resource_json("metadata.json")

    assert actual_metadata == expected_metadata


def test_get_creators():
    response_json = _get_resource_json("artists_details.json")
    actual_name = bkm._get_creators(response_json)
    expected_name = "John La Farge"

    assert actual_name == expected_name


def test_get_no_creators():
    data = {}
    actual_name = bkm._get_creators(data)
    expected_name = None

    assert actual_name == expected_name


def test_get_images():
    response_json = _get_resource_json("image_details.json")
    actual_image_url, actual_thumbnail_url = bkm._get_images(response_json)
    expected_image_url = "https://d1lfxha3ugu3d4.cloudfront.net/images/opencollection/objects/size4/CUR.66.242.29.jpg"
    expected_thumbnail_url = "https://d1lfxha3ugu3d4.cloudfront.net/images/opencollection/objects/size0_sq/CUR.66.242.29.jpg"

    assert actual_image_url == expected_image_url
    assert actual_thumbnail_url == expected_thumbnail_url


def test_get_no_images():
    data = {}
    actual_image_url, actual_thumbnail_url = bkm._get_images(data)
    expected_image_url = None
    expected_thumbnail_url = None

    assert actual_image_url == expected_image_url
    assert actual_thumbnail_url == expected_image_url

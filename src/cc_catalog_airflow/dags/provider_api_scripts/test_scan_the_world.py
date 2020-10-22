import json
import logging
import os
import requests
from unittest.mock import patch, MagicMock

import scan_the_world as stw

RESOURCES = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), "tests/resources/stw"
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s",
    level=logging.DEBUG,
)


def _get_resource_json(json_name):
    with open(os.path.join(RESOURCES, json_name)) as f:
        resource_json = json.load(f)
    return resource_json


def test_get_object_list_retries_with_none_response():
    with patch.object(stw.delayed_requester, "get", return_value=None) as mock_get:
        stw._get_object_list("some param", retries=3)

    assert mock_get.call_count == 3


def test_get_object_list_retries_with_non_ok_response():
    response_json = _get_resource_json("stw_example.json")
    r = requests.Response()
    r.status_code = 504
    r.json = MagicMock(return_value=response_json)
    with patch.object(stw.delayed_requester, "get", return_value=r) as mock_get:
        stw._get_object_list("some param", retries=3)

    assert mock_get.call_count == 3


def test_get_object_list_with_realistic_response():
    response_json = _get_resource_json("stw_example.json")
    r = requests.Response()
    r.status_code = 200
    r.json = MagicMock(return_value=response_json)
    with patch.object(stw.delayed_requester, "get", return_value=r) as mock_get:
        object_list = stw._get_object_list("test", retries=3)
    expect_object_list = _get_resource_json("stw_example_object_list.json")

    assert mock_get.call_count == 1
    assert object_list == expect_object_list


def test_build_query_param_dict():
    actual_query_param_dict = stw._get_query_param(
        page=1,
    )
    expect_query_param_dict = {"cat": 112, "rec_cat": 1, "page": 1, "per_page": 100}
    assert actual_query_param_dict == expect_query_param_dict


def test_extract_image_list_from_json_handles_realistic_input():
    test_dict = _get_resource_json("stw_example.json")
    expect_object_list = _get_resource_json("stw_example_object_list.json")

    actual_object_list = stw._extract_object_list_from_json(test_dict)

    assert actual_object_list == expect_object_list


def test_extract_object_list_from_json_handles_empty_item_list():
    test_dict = {"items": []}
    expect_list = []
    assert stw._extract_object_list_from_json(test_dict) == expect_list


def test_extract_object_list_from_json_handles_missing_item():
    test_dict = {"abc": "def"}
    assert stw._extract_object_list_from_json(test_dict) is None


def test_extract_object_list_from_json_handle_empty_json():
    test_dict = {}
    assert stw._extract_object_list_from_json(test_dict) is None


def test_extract_object_list_from_json_returns_nones_given_none_json():
    assert stw._extract_object_list_from_json(None) is None


def test_process_object_with_real_example():
    object_data = _get_resource_json("object_complete_example.json")
    with patch.object(stw.image_store, "add_item", return_value=100) as mock_add_item:
        total_images = stw._process_object(object_data)

    expect_meta_data = {
        "pub_date": "2017-12-15T19:57:12+00:00",
        "likes": 493,
        "views": 47710,
        "description": (
            "This is a scan from a cast of the original David by Michelangelo, a masterpiece of Renaissance art."
        ),
    }

    mock_add_item.assert_called_once_with(
        foreign_landing_url=(
            "https://www.myminifactory.com/object/3d-print-head-of-michelangelo-s-david-52645"
        ),
        image_url=(
            "https://cdn.myminifactory.com/assets/object-assets/5a34065096e57/images/head-of-david.jpg"
        ),
        thumbnail_url=(
            "https://cdn.myminifactory.com/assets/object-assets/5a34065096e57/images/230X230-head-of-david.jpg"
        ),
        license_="cc0",
        license_version="1.0",
        foreign_identifier=536487,
        creator_url="https://www.myminifactory.com/users/SMK%20-%20Statens%20Museum%20for%20Kunst",
        title=("Head of Michelangelo's David"),
        meta_data=expect_meta_data,
        raw_tags=[
            "3d",
            "david",
            "head",
            "sculpture",
            "Renaissance",
            "Michelangelo",
            "Plaster",
            "scan",
            "cast",
            "artec",
        ],
    )
    assert total_images == 100


def test_process_object_invalid_license():
    object_data = _get_resource_json("non_cc_license.json")
    assert stw._process_object(object_data) is None


def test_get_object_meta_data_returns_full_meta_data_given_right_input():
    response_json = _get_resource_json("full_object_meta.json")
    actual_metadata = stw._create_meta_data_dict(response_json)
    expected_metadata = {
        "pub_date": "2017-12-15T19:57:12+00:00",
        "likes": 493,
        "views": 47710,
        "description": (
            "This is a scan from a cast of the original David by Michelangelo, a masterpiece of Renaissance art."
        ),
    }

    assert actual_metadata == expected_metadata


def test_get_object_meta_data_return_empty_dict_given_no_meta_data():
    response_json = _get_resource_json("no_meta_data.json")
    actual_metadata = stw._create_meta_data_dict(response_json)
    expected_metadata = {}
    assert actual_metadata == expected_metadata

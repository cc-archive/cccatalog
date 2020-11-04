import os
import json
import logging
import requests
from unittest.mock import patch, MagicMock
import noun_project as nounpro


RESOURCES = os.path.join(
    os.path.abspath(os.path.dirname(__file__)),
    'tests/resources/nounproject'
)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.DEBUG
)

logger = logging.getLogger(__name__)


def _get_resource_json(json_resource):
    with open(os.path.join(RESOURCES, json_resource)) as file:
        json_resource = json.load(file)
    return json_resource


# _get_collections_list test suite
def test_get_collection_list_retries_with_none_response():
    with patch.object(
            nounpro.delayed_requester,
            'get_response_json',
            return_value=None
    ) as mock_get:
        nounpro._get_collections_list(retries=3)

    assert mock_get.call_count == 1


def test_get_collections_list_retries_with_non_ok_response():
    response_json = _get_resource_json('collections_full_response_example.json')
    r = requests.Response()
    r.status_code = 504
    r.json = MagicMock(return_value=response_json)
    with patch.object(
            nounpro.delayed_requester,
            'get_response_json',
            return_value=r.json()
    ) as mock_get:
        nounpro._get_collections_list(total_pages=1)

    assert mock_get.call_count == 1


def test_get_collections_list_with_full_response():
    response_json = _get_resource_json('collections_full_response_example.json')
    r = requests.Response()
    r.status_code = 200
    r.json = MagicMock(return_value=response_json)
    with patch.object(
            nounpro.delayed_requester,
            'get_response_json',
            return_value=r.json()
    ) as mock_get:
        collections_list = nounpro._get_collections_list(total_pages=1)

    expect_collections_list = _get_resource_json('collections_list_example.json')
    assert mock_get.call_count == 1
    assert collections_list == expect_collections_list


# _extract_collections_list_from_json test suite
def test_extract_collections_list_from_json_returns_expected_output():
    json_response_inpydict_form = _get_resource_json(
        "collections_full_response_example.json"
    )
    actual_collections_list = nounpro._extract_collections_list_from_json(
        json_response_inpydict_form
    )
    expected_collections_list = _get_resource_json('collections_list_example.json')
    assert actual_collections_list == expected_collections_list


def test_extract_collections_list_from_json_handles_missing_collections():
    test_dict = {}
    assert nounpro._extract_collections_list_from_json(test_dict) is None


def test_extract_collections_list_from_json_handles_missing_collections_in_collections():
    test_dict = {
        "collections": []
    }
    assert nounpro._extract_collections_list_from_json(test_dict) is None


def test_extract_collections_list_from_json_returns_nones_given_none_json():
    assert nounpro._extract_collections_list_from_json(None) is None


# _get_icons_list_from_collection test suite
def test_get_icons_list_from_collection_retries_with_none_response():
    with patch.object(
            nounpro.delayed_requester,
            'get_response_json',
            return_value=None
    ) as mock_get:
        nounpro._get_icons_list_from_collection(collection='some_collection', retries=3)

    assert mock_get.call_count == 1


def test_get_icons_list_from_collection_retries_with_non_ok_response():
    response_json = _get_resource_json('full_icon_list_response_example.json')
    r = requests.Response()
    r.status_code = 504
    r.json = MagicMock(return_value=response_json)
    with patch.object(
            nounpro.delayed_requester,
            'get_response_json',
            return_value=r.json()
    ) as mock_get:
        nounpro._get_icons_list_from_collection(collection='some_collection')

    assert mock_get.call_count == 4


def test_get_icons_list_from_collection_with_full_response():
    response_json = _get_resource_json('full_icon_list_response_example.json')
    r = requests.Response()
    r.status_code = 200
    r.json = MagicMock(return_value=response_json)
    with patch.object(
            nounpro.delayed_requester,
            'get_response_json',
            return_value=r.json()
    ) as mock_get:
        icons_list = nounpro._get_icons_list_from_collection(collection='national-park-service')

    expect_icons_list = _get_resource_json('icons_list_example_with_collection_appended.json')*3
    assert mock_get.call_count == 4
    assert icons_list == expect_icons_list


# _build_query_param_icon_list test suite
def test_build_query_param():
    actual_param_made = nounpro._build_query_param(page=1)
    expected_param = {"page": 1}

    assert actual_param_made == expected_param


# _extract_icons_list_from_json test suite
def test_extract_icons_list_from_json_returns_expected_output():
    json_response_inpydict_form = _get_resource_json(
        "noun_project_full_collection_example.json"
    )
    actual_icons_list = nounpro._extract_icons_list_from_json(
        json_response_inpydict_form
    )
    expected_icons_list = _get_resource_json('icons_list_example.json')
    assert actual_icons_list == expected_icons_list


def test_extract_icons_list_from_json_handles_missing_icons():
    test_dict = {}
    assert nounpro._extract_icons_list_from_json(test_dict) is None


def test_extract_icons_list_from_json_handles_missing_icons_in_icons():
    test_dict = {
        "icons": []
    }
    assert nounpro._extract_icons_list_from_json(test_dict) is None


def test_extract_icons_list_from_json_returns_nones_given_none_json():
    assert nounpro._extract_icons_list_from_json(None) is None


# _process_icon test suite
def test_process_icon_returns_expected_output_given_right_input():
    icon = _get_resource_json('full_icon_object.json')
    with patch.object(
        nounpro.image_store,
        'add_item',
        return_value=100
    ) as mock_add_item:
        total_images = nounpro._process_icon(icon)

    mock_add_item.assert_called_once_with(
        foreign_landing_url="https://thenounproject.com/national-park-service/collection/national-park-service/?i=19",
        image_url="https://static.thenounproject.com/noun-svg/19.svg?Expires=1603780287&Signature=ll-gd8lBm09wKxOYSqnZcvjVMrg-JjrBIiF3oLBCu-nqK7UVE5mGMnhiJ9SX80Zy7VLshF3tb5tcNaoWNkW1auaKGOBVf4S1wb41QIQ~EY5I~A1aZWYOWOnMq1F-RoWiIX1Db4Hf7myhrN~ahNwmJ9lrOPPyQi-Y~NXwoTosFEo_&Key-Pair-Id=APKAI5ZVHAXN65CHVU2Q",
        license_url="https://creativecommons.org/publicdomain/zero/1.0/",
        thumbnail_url="https://static.thenounproject.com/png/19-84.png",
        foreign_identifier="19",
        creator="National Park Service",
        creator_url="https://thenounproject.com/national-park-service/",
        title="tunnel",
        raw_tags=[{"id": 46, "slug": "tunnel"}],
        watermarked=False
    )

    assert total_images == 100


# _get_foreign_landing_url test suite
def test_get_foreign_landing_url_returns_expected_output_given_right_input():
    response_json = _get_resource_json('full_icon_object.json')
    actual_foreign_landing_url = nounpro._get_foreign_landing_url(response_json)
    expected_foreign_landing_url = "https://thenounproject.com/national-park-service/collection/national-park-service/?i=19"

    assert actual_foreign_landing_url == expected_foreign_landing_url


def test_get_foreign_landing_url_returns_none_given_no_foreign_landing_url():
    response_json = _get_resource_json('no_foreign_landing_url.json')
    actual_foreign_landing_url = nounpro._get_foreign_landing_url(response_json)
    expected_foreign_landing_url = None

    assert actual_foreign_landing_url == expected_foreign_landing_url


# get_license_url test suite
def test_get_license_url_returns_expected_output_given_right_input():
    response_json = _get_resource_json('full_icon_object.json')
    actual_license_url = nounpro._get_license_url(response_json)
    expected_license_url = "https://creativecommons.org/publicdomain/zero/1.0/"

    assert actual_license_url == expected_license_url


def test_get_license_url_returns_none_given_no_license_url():
    response_json = _get_resource_json('no_license_url.json')
    actual_license_url = nounpro._get_license_url(response_json)
    expected_license_url = None

    assert actual_license_url == expected_license_url


# _get_creator_url test suite
def test_get_creator_url_returns_expected_output_given_right_input():
    response_json = _get_resource_json('full_icon_object.json')
    actual_creator_url = nounpro._get_creator_url(response_json)
    expected_creator_url = "https://thenounproject.com/national-park-service/"

    assert actual_creator_url == expected_creator_url


def test_get_creator_url_returns_none_given_no_creator_url():
    response_json = _get_resource_json('no_creator_url.json')
    actual_creator_url = nounpro._get_creator_url(response_json)
    expected_creator_url = None

    assert actual_creator_url == expected_creator_url

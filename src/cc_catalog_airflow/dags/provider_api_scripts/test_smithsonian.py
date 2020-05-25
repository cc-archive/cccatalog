import json
import logging
import os
from unittest.mock import patch, call

import pytest

import smithsonian as si

logger = logging.getLogger(__name__)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.DEBUG
)

RESOURCES = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'tests/resources/smithsonian'
)


def test_get_hash_prefixes_with_len_one():
    expect_prefix_list = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
        'e', 'f'
    ]
    actual_prefix_list = list(si._get_hash_prefixes(1))
    assert actual_prefix_list == expect_prefix_list


@pytest.mark.parametrize(
    "input_int,expect_len,expect_first,expect_last",
    [
        (1, 16, '0', 'f'),
        (2, 256, '00', 'ff'),
        (3, 4096, '000', 'fff'),
        (4, 65536, '0000', 'ffff'),
    ],
)
def test_get_hash_prefixes_with_other_len(
        input_int,
        expect_len,
        expect_first,
        expect_last
):
    actual_list = list(si._get_hash_prefixes(input_int))
    assert all('0x' not in h for h in actual_list)
    assert all(
        int(actual_list[i + 1], 16) - int(actual_list[i], 16) == 1
        for i in range(len(actual_list) - 1)
    )
    assert len(actual_list) == expect_len
    assert actual_list[0] == expect_first
    assert actual_list[-1] == expect_last


def test_process_hash_prefix_with_none_response_json():
    endpoint = 'https://abc.com/123'
    limit = 100
    hash_prefix = '00'
    retries = 3
    qp = {'q': 'abc'}

    patch_get_response_json = patch.object(
        si.delayed_requester,
        'get_response_json',
        return_value=None
    )
    patch_build_qp = patch.object(
        si,
        '_build_query_params',
        return_value=qp
    )
    patch_process_response = patch.object(
        si,
        '_process_response_json'
    )
    with\
            patch_get_response_json as mock_get_response_json,\
            patch_build_qp as mock_build_qp,\
            patch_process_response as mock_process_response:
        si._process_hash_prefix(
            hash_prefix,
            endpoint=endpoint,
            limit=limit,
            retries=retries
        )
    mock_process_response.assert_not_called()
    mock_build_qp.assert_called_once_with(0, hash_prefix=hash_prefix)
    mock_get_response_json.assert_called_once_with(
        endpoint,
        retries=retries,
        query_params=qp
    )


def test_process_hash_prefix_with_response_json_no_row_count():
    endpoint = 'https://abc.com/123'
    limit = 100
    hash_prefix = '00'
    retries = 3
    qp = {'q': 'abc'}
    response_json = {'abc': '123'}

    patch_get_response_json = patch.object(
        si.delayed_requester,
        'get_response_json',
        return_value=response_json
    )
    patch_build_qp = patch.object(
        si,
        '_build_query_params',
        return_value=qp
    )
    patch_process_response = patch.object(
        si,
        '_process_response_json'
    )
    with\
            patch_get_response_json as mock_get_response_json,\
            patch_build_qp as mock_build_qp,\
            patch_process_response as mock_process_response:
        si._process_hash_prefix(
            hash_prefix,
            endpoint=endpoint,
            limit=limit,
            retries=retries
        )
    mock_process_response.assert_called_with(response_json)
    mock_build_qp.assert_called_once_with(0, hash_prefix=hash_prefix)
    mock_get_response_json.assert_called_once_with(
        endpoint,
        retries=retries,
        query_params=qp
    )


def test_process_hash_prefix_with_good_response_json():
    endpoint = 'https://abc.com/123'
    limit = 100
    hash_prefix = '00'
    retries = 3
    qp = {'q': 'abc'}
    response_json = {
        'response': {
            'abc': '123',
            'rowCount': 150
        }
    }

    patch_get_response_json = patch.object(
        si.delayed_requester,
        'get_response_json',
        return_value=response_json
    )
    patch_build_qp = patch.object(
        si,
        '_build_query_params',
        return_value=qp
    )
    patch_process_response = patch.object(
        si,
        '_process_response_json',
        return_value=0
    )
    with\
            patch_build_qp as mock_build_qp,\
            patch_get_response_json as mock_get_response_json,\
            patch_process_response as mock_process_response:
        si._process_hash_prefix(
            hash_prefix,
            endpoint=endpoint,
            limit=limit,
            retries=retries
        )
    expect_process_response_calls = [call(response_json), call(response_json)]
    expect_build_qp_calls = [
        call(0, hash_prefix=hash_prefix),
        call(limit, hash_prefix=hash_prefix)
    ]
    expect_get_response_json_calls = [
        call(endpoint, retries=retries, query_params=qp),
        call(endpoint, retries=retries, query_params=qp)
    ]
    mock_build_qp.assert_has_calls(expect_build_qp_calls)
    mock_get_response_json.assert_has_calls(expect_get_response_json_calls)
    mock_process_response.assert_has_calls(expect_process_response_calls)


def test_build_query_params():
    hash_prefix = 'ff'
    row_offset = 10
    default_params = {
        'api_key': 'pass123',
        'rows': 10
    }
    acutal_params = si._build_query_params(
        row_offset,
        hash_prefix=hash_prefix,
        default_params=default_params
    )
    expect_params = {
        'api_key': 'pass123',
        'rows': 10,
        'q': f'online_media_type:Images AND media_usage:CC0 AND hash:{hash_prefix}*',
        'start': row_offset
    }
    assert acutal_params == expect_params
    assert default_params == {
        'api_key': 'pass123',
        'rows': 10
    }


def test_process_response_json_with_no_rows_json():
    response_json = {
        'status': 200,
        'responseCode': 1,
        'response': {
            'norows': ['abc', 'def'],
            'rowCount': 2,
            'message': 'content found'
        }
    }
    patch_process_image_list = patch.object(
        si,
        '_process_image_list',
        return_value=2
    )

    with patch_process_image_list as mock_process_image_list:
        si._process_response_json(response_json)

    mock_process_image_list.assert_not_called()


def test_process_response_json_uses_row_list_getter_function():
    """
    This test only checks for appropriate calls to _get_row_list
    """
    response_json = {
        'test key': 'test value'
    }
    patch_get_row_list = patch.object(
        si,
        '_get_row_list',
        return_value=[]
    )

    with patch_get_row_list as mock_get_row_list:
        si._process_response_json(response_json)

    mock_get_row_list.assert_called_once_with(response_json)


def test_process_response_json_uses_required_getters():
    """
    This test only checks for appropriate calls to _get_row_list
    """
    response_json = {}
    patch_get_row_list = patch.object(
        si,
        '_get_row_list',
        return_value=['row1', 'row2']
    )
    patch_process_image_list = patch.object(
        si,
        '_process_image_list',
        return_value=2
    )
    get_image_list = patch.object(si, '_get_image_list', return_value=None)
    get_flu = patch.object(si, '_get_foreign_landing_url', return_value=None)
    get_title = patch.object(si, '_get_title', return_value=None)
    get_creator = patch.object(si, '_get_creator', return_value=None)
    ext_meta_data = patch.object(si, '_extract_meta_data', return_value=None)
    ext_tags = patch.object(si, '_extract_tags', return_value=None)

    with\
            patch_get_row_list as mock_get_row_list,\
            get_image_list as mock_get_image_list,\
            get_flu as mock_get_foreign_landing_url,\
            get_title as mock_get_title,\
            get_creator as mock_get_creator,\
            ext_meta_data as mock_extract_meta_data,\
            ext_tags as mock_extract_tags,\
            patch_process_image_list as mock_process_image_list:
        si._process_response_json(response_json)

    calls_list = [call('row1'), call('row2')]
    mock_get_row_list.assert_called_once_with(response_json)
    mock_process_image_list.assert_not_called()
    mock_get_foreign_landing_url.assert_has_calls(calls_list)
    mock_get_title.assert_has_calls(calls_list)
    mock_get_creator.assert_has_calls(calls_list)
    mock_extract_meta_data.assert_has_calls(calls_list)
    mock_extract_tags.assert_has_calls(calls_list)
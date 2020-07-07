import logging
import requests

import pytest
import tldextract

from common.licenses import licenses

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.DEBUG)

licenses.urls.tldextract.extract = tldextract.TLDExtract(
    suffix_list_urls=None
)


@pytest.fixture
def get_good(monkeypatch):
    def mock_get(url, timeout=60):
        return requests.Response()
    monkeypatch.setattr(licenses.requests, 'get', mock_get)


@pytest.fixture
def get_bad(monkeypatch):
    def mock_get(url, timeout=60):
        raise Exception
    monkeypatch.setattr(licenses.requests, 'get', mock_get)


@pytest.fixture
def mock_rewriter(monkeypatch):
    def mock_rewrite_redirected_url(url_string):
        return url_string
    monkeypatch.setattr(
        licenses.urls, 'rewrite_redirected_url', mock_rewrite_redirected_url
    )


def test_get_license_info_prefers_derived_values(monkeypatch):
    expected_license, expected_version = 'derivedlicense', '10.0'
    expected_url = 'https://creativecommons.org/licenses/and/so/on'

    def mock_cc_license_validator(url_string):
        return url_string

    def mock_derive_license(url_string, path_map=None):
        return expected_license, expected_version

    def mock_validate_pair(license_, license_version):
        return license_, license_version

    monkeypatch.setattr(
        licenses, '_get_valid_cc_license_url', mock_cc_license_validator
    )

    monkeypatch.setattr(
        licenses, '_derive_license_from_url', mock_derive_license
    )
    monkeypatch.setattr(
        licenses, '_validate_license_pair', mock_validate_pair
    )

    actual_license, actual_version, actual_url = licenses.get_license_info(
        expected_url,
        'license',
        '1.0'
    )

    assert actual_license == expected_license
    assert actual_version == expected_version
    assert actual_url == expected_url


def test_derive_license_from_url_with_license_url_path_mismatch(monkeypatch):
    license_url = 'https://not.in/path/map'
    path_map = {'zero/1.0': {'license': 'cc0', 'version': '1.0'}}
    with pytest.raises(licenses.InvalidLicenseURLException):
        licenses._derive_license_from_url(license_url, path_map=path_map)


def test_derive_license_from_url_with_good_license_url(monkeypatch):
    expected_license, expected_version = 'cc0', '1.0'
    license_url = 'https://creativecommons.org/licenses/zero/1.0'
    path_map = {'zero/1.0': {'license': 'cc0', 'version': '1.0'}}
    actual_license, actual_version = licenses._derive_license_from_url(
        license_url, path_map=path_map
    )
    assert actual_license == expected_license
    assert actual_version == expected_version


def test_derive_license_from_url_finds_correct_nonstandard_info(mock_rewriter):
    actual_license, actual_version = licenses._derive_license_from_url(
        'http://creativecommons.org/publicdomain/zero/1.0/',
    )
    expect_license, expect_version = 'cc0', '1.0'
    assert actual_license == expect_license
    assert actual_version == expect_version


def test_get_valid_cc_url_makes_url_lowercase(mock_rewriter):
    actual_url = licenses._get_valid_cc_license_url(
        'http://creativecommons.org/licenses/CC0/1.0/legalcode',
    )
    expect_url = actual_url.lower()
    assert actual_url == expect_url


def test_get_valid_cc_license_url_nones_wrong_domain(mock_rewriter):
    actual_url = licenses._get_valid_cc_license_url(
        'http://notcreativecommons.org/licenses/by/1.0/',
    )
    assert actual_url is None


def test_get_valid_cc_license_url_nones_missing_url(mock_rewriter):
    actual_url = licenses._get_valid_cc_license_url(None)
    assert actual_url is None


def test_validate_license_pair_nones_missing_license():
    path_map = {'by/1.0': {'license': 'by', 'version': '1.0'}}
    actual_license, actual_version = licenses._validate_license_pair(
        None,
        '1.0',
        path_map
    )
    expect_license, expect_version = None, None
    assert actual_license == expect_license
    assert actual_version == expect_version


def test_validate_license_pair_nones_missing_version():
    path_map = {'by/1.0': {'license': 'by', 'version': '1.0'}}
    actual_license, actual_version = licenses._validate_license_pair(
        'by',
        None,
        path_map
    )
    expect_license, expect_version = None, None
    assert actual_license == expect_license
    assert actual_version == expect_version


def test_validate_license_pair_handles_float_version():
    path_map = {'by/1.0': {'license': 'by', 'version': '1.0'}}
    actual_license, actual_version = licenses._validate_license_pair(
        'by',
        1.0,
        path_map
    )
    expect_license, expect_version = 'by', '1.0'
    assert actual_license == expect_license
    assert actual_version == expect_version


def test_validate_license_pair_handles_int_version():
    path_map = {'by/1.0': {'license': 'by', 'version': '1.0'}}
    actual_license, actual_version = licenses._validate_license_pair(
        'by',
        1,
        path_map
    )
    expect_license, expect_version = 'by', '1.0'
    assert actual_license == expect_license
    assert actual_version == expect_version
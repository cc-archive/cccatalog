import os

from airflow.hooks.postgres_hook import PostgresHook
import pytest

from util.loader import test_sql
from util import pg_cleaner

RESOURCES = os.path.join(
    os.path.abspath(os.path.dirname(__file__)), 'test_resources'
)

TEST_IMAGE_TABLE = test_sql.TEST_IMAGE_TABLE
DROP_IMAGE_TABLE_QUERY = test_sql.DROP_IMAGE_TABLE_QUERY
DROP_IMAGE_INDEX_QUERY = test_sql.DROP_IMAGE_INDEX_QUERY
UUID_FUNCTION_QUERY = test_sql.UUID_FUNCTION_QUERY
CREATE_IMAGE_TABLE_QUERY = test_sql.CREATE_IMAGE_TABLE_QUERY
UNIQUE_CONDITION_QUERY = test_sql.UNIQUE_CONDITION_QUERY


POSTGRES_CONN_ID = os.getenv('TEST_CONN_ID')
POSTGRES_TEST_URI = os.getenv('AIRFLOW_CONN_POSTGRES_OPENLEDGER_TESTING')


@pytest.fixture
def postgres_with_image_table():
    postgres = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    postgres.run(DROP_IMAGE_TABLE_QUERY)
    postgres.run(DROP_IMAGE_INDEX_QUERY)
    postgres.run(UUID_FUNCTION_QUERY)
    postgres.run(CREATE_IMAGE_TABLE_QUERY)
    postgres.run(UNIQUE_CONDITION_QUERY)

    yield postgres

    postgres.run(DROP_IMAGE_TABLE_QUERY)
    postgres.run(DROP_IMAGE_INDEX_QUERY)


@pytest.fixture
def mock_unchangers(monkeypatch):
    def mock_get_license_info(
            license_url=None, license_=None, license_version=None
    ):
        return pg_cleaner.image.licenses.LicenseInfo(
            license=license_, version=license_version, url=license_url
        )
    monkeypatch.setattr(
        pg_cleaner.image.licenses,
        'get_license_info',
        mock_get_license_info,
    )

    def mock_validate_url_string(
            url_string
    ):
        return url_string
    monkeypatch.setattr(
        pg_cleaner.image.columns.urls,
        'validate_url_string',
        mock_validate_url_string,
    )


@pytest.fixture
def mock_breakers(monkeypatch):
    def mock_get_license_info(
            license_url=None, license_=None, license_version=None
    ):
        assert 0 == 1
    monkeypatch.setattr(
        pg_cleaner.image.licenses,
        'get_license_info',
        mock_get_license_info,
    )

    def mock_validate_url_string(
            url_string
    ):
        assert 0 == 1
    monkeypatch.setattr(
        pg_cleaner.image.columns.urls,
        'validate_url_string',
        mock_validate_url_string,
    )


def _load_tsv(postgres, tmpdir, tsv_file_name):
    tsv_file_path = os.path.join(RESOURCES, tsv_file_name)
    with open(tsv_file_path) as f:
        f_data = f.read()

    test_tsv = 'test.tsv'
    path = tmpdir.join(test_tsv)
    path.write(f_data)
    postgres.bulk_load(TEST_IMAGE_TABLE, str(path))


def test_clean_rows_is_idempotent(
        tmpdir, postgres_with_image_table, mock_unchangers,
):
    tsv_name = os.path.join(RESOURCES, 'image_table_sample.tsv')
    _load_tsv(postgres_with_image_table, tmpdir, tsv_name)
    expected_records = postgres_with_image_table.get_records(
        f"SELECT * FROM {TEST_IMAGE_TABLE}"
    )
    total_cleaned = pg_cleaner.clean_rows(
        POSTGRES_CONN_ID,
        '0',
        image_table=TEST_IMAGE_TABLE
    )
    assert total_cleaned == len(expected_records)
    actual_records = postgres_with_image_table.get_records(
        f"SELECT * FROM {TEST_IMAGE_TABLE}"
    )
    assert expected_records == actual_records


def test_clean_rows_exits_when_cleaning_fails(
        tmpdir, postgres_with_image_table, mock_breakers,
):
    tsv_name = os.path.join(RESOURCES, 'image_table_sample.tsv')
    _load_tsv(postgres_with_image_table, tmpdir, tsv_name)
    expected_records = postgres_with_image_table.get_records(
        f"SELECT * FROM {TEST_IMAGE_TABLE}"
    )
    with pytest.raises(SystemExit) as wrapped:
        pg_cleaner.clean_rows(
            POSTGRES_CONN_ID,
            '0',
            image_table=TEST_IMAGE_TABLE
        )
    assert wrapped.value.code == 1
    actual_records = postgres_with_image_table.get_records(
        f"SELECT * FROM {TEST_IMAGE_TABLE}"
    )
    assert expected_records == actual_records

"""
This file provides the pieces to perform an after-the-fact processing
of all data in the image table of the upstream DB through the ImageStore
class.
"""
from collections import namedtuple
import logging
import sys
from textwrap import dedent
import time

from airflow.hooks.postgres_hook import PostgresHook

from provider_api_scripts.common.storage import image
from util import tsv_cleaner
from util.loader import column_names as col
from util.loader.sql import IMAGE_TABLE_NAME

logger = logging.getLogger(__name__)

IMAGE_TABLE_COLS = [
    # These are not precisely the same names as in the DB.
    "identifier",
    "created_on",
    "updated_on",
    "ingestion_type",
    "provider",
    "source",
    "foreign_identifier",
    "foreign_landing_url",
    "image_url",
    "thumbnail_url",
    "width",
    "height",
    "filesize",
    "license_",
    "license_version",
    "creator",
    "creator_url",
    "title",
    "meta_data",
    "tags",
    "watermarked",
    "last_synced",
    "removed",
]

ImageTableRow = namedtuple("ImageTableRow", IMAGE_TABLE_COLS)


class ImageCleaner(image.ImageStore):

    def clean_image(
            self,
            foreign_landing_url=None,
            image_url=None,
            thumbnail_url=None,
            license_url=None,
            license_=None,
            license_version=None,
            foreign_identifier=None,
            width=None,
            height=None,
            creator=None,
            creator_url=None,
            title=None,
            meta_data=None,
            raw_tags=None,
            watermarked='f',
            source=None,
    ):
        image = self._get_image(
            foreign_landing_url=foreign_landing_url,
            image_url=image_url,
            thumbnail_url=thumbnail_url,
            license_url=license_url,
            license_=license_,
            license_version=license_version,
            foreign_identifier=foreign_identifier,
            width=width,
            height=height,
            creator=creator,
            creator_url=creator_url,
            title=title,
            meta_data=meta_data,
            raw_tags=raw_tags,
            watermarked=watermarked,
            source=source
        )
        cleaned_row_list = [
            self._finish_string(s) for s in self._prepare_valid_row_list(image)
        ]
        return cleaned_row_list

    def _finish_string(self, s):
        return f"""'{s.replace("'", "''")}'""" if s is not None else "null"


class ImageCleanerDict(dict):

    def __missing__(self, key):
        ret = self[key] = ImageCleaner(provider=key)
        return ret


_image_cleaner_dict = ImageCleanerDict()


def clean_rows(postgres_conn_id, prefix, image_table=IMAGE_TABLE_NAME):
    """
    This function runs all rows from the image table whose identifier
    starts with the given prefix through the ImageCleaner class, and
    updates them with the result.
    """
    start_time = time.time()
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    select_query = _get_select_query_from_prefix(prefix, image_table)
    selected_rows = postgres.get_records(select_query)
    total_cleaned = 0
    for record in selected_rows:
        try:
            clean_record = _get_clean_row_tuple(record)
            update_query = _get_update_query_for_record(
                record[0], clean_record
            )
            postgres.run(update_query)
            total_cleaned += 1
        except Exception as e:
            logger.error(
                f"The record {record} could not be cleaned."
                f"\nError: {e}"
                "\nAbort!"
            )
            sys.exit(1)

    end_time = time.time()
    logger.info(
        f"{total_cleaned} records cleaned in {end_time - start_time} seconds"
    )
    return total_cleaned


def _get_select_query_from_prefix(prefix, image_table):
    """
    This creates the necessary string to select all rows from the image
    table where the identifier matches the given prefix.
    """
    min_base_uuid = '00000000-0000-0000-0000-000000000000'
    max_base_uuid = 'ffffffff-ffff-ffff-ffff-ffffffffffff'
    min_uuid = prefix + min_base_uuid[len(prefix):]
    max_uuid = prefix + max_base_uuid[len(prefix):]
    select_query = dedent(
        f"""
        SELECT
          {col.IDENTIFIER}, {col.CREATED_ON}, {col.UPDATED_ON},
          {col.INGESTION_TYPE}, {col.PROVIDER}, {col.SOURCE}, {col.FOREIGN_ID},
          {col.LANDING_URL}, {col.DIRECT_URL}, {col.THUMBNAIL}, {col.WIDTH},
          {col.HEIGHT}, {col.FILESIZE}, {col.LICENSE}, {col.LICENSE_VERSION},
          {col.CREATOR}, {col.CREATOR_URL}, {col.TITLE}, {col.META_DATA},
          {col.TAGS}, {col.WATERMARKED}, {col.LAST_SYNCED}, {col.REMOVED}
        FROM {image_table}
        WHERE
          {col.IDENTIFIER}>='{min_uuid}'::uuid
          AND
          {col.IDENTIFIER}<='{max_uuid}'::uuid;
        """
    )
    return select_query


def _get_clean_row_tuple(orig_row_tuple):
    """
    This function should take a tuple representing a row of the image
    table, run the appropriate fields through the ImageStore, and then
    return the resulting cleaned tuple.
    """
    dirty_row = ImageTableRow(*orig_row_tuple)
    image_cleaner = _image_cleaner_dict[dirty_row.provider]
    clean_fields = image_cleaner.clean_image(
        foreign_landing_url=dirty_row.foreign_landing_url,
        image_url=dirty_row.image_url,
        thumbnail_url=dirty_row.thumbnail_url,
        license_url=tsv_cleaner.get_license_url(dirty_row.meta_data),
        license_=dirty_row.license_,
        license_version=dirty_row.license_version,
        foreign_identifier=dirty_row.foreign_identifier,
        width=dirty_row.width,
        height=dirty_row.height,
        creator=dirty_row.creator,
        creator_url=dirty_row.creator_url,
        title=dirty_row.title,
        meta_data=dirty_row.meta_data,
        raw_tags=dirty_row.tags,
        watermarked=dirty_row.watermarked,
        source=dirty_row.source,
    )
    return image.Image(*clean_fields)


def _get_update_query_for_record(
        identifier, record, image_table=IMAGE_TABLE_NAME
):
    update_query = dedent(
        f"""
        UPDATE {image_table}
        SET
          {col.PROVIDER}={record.provider},
          {col.SOURCE}={record.source},
          {col.FOREIGN_ID}={record.foreign_identifier},
          {col.LANDING_URL}={record.foreign_landing_url},
          {col.DIRECT_URL}={record.image_url},
          {col.THUMBNAIL}={record.thumbnail_url},
          {col.WIDTH}={record.width},
          {col.HEIGHT}={record.height},
          {col.FILESIZE}={record.filesize},
          {col.LICENSE}={record.license_},
          {col.LICENSE_VERSION}={record.license_version},
          {col.CREATOR}={record.creator},
          {col.CREATOR_URL}={record.creator_url},
          {col.TITLE}={record.title},
          {col.META_DATA}={record.meta_data},
          {col.TAGS}={record.tags},
          {col.WATERMARKED}={record.watermarked}
        WHERE {col.IDENTIFIER}='{identifier}'::uuid;
        """
    )
    return update_query


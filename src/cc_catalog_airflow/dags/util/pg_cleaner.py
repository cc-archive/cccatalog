"""
This file provides the pieces to perform an after-the-fact processing
of all data in the image table of the upstream DB through the ImageStore
class.
"""
from collections import namedtuple
import logging

from provider_api_scripts.common.storage import image
from util import tsv_cleaner
from util.loader import column_names as col

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
        return self._prepare_valid_row_list(image)


class ImageCleanerDict(dict):

    def __missing__(self, key):
        ret = self[key] = ImageCleaner(provider=key)
        return ret


_image_cleaner_dict = ImageCleanerDict()


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
    clean_image = image.Image(*clean_fields)
    clean_row = dirty_row._replace(
        provider=clean_image.provider,
        source=clean_image.source,
        foreign_identifier=clean_image.foreign_identifier,
        foreign_landing_url=clean_image.foreign_landing_url,
        image_url=clean_image.image_url,
        thumbnail_url=clean_image.thumbnail_url,
        width=clean_image.width,
        height=clean_image.height,
        filesize=clean_image.filesize,
        license_=clean_image.license_,
        license_version=clean_image.license_version,
        creator=clean_image.creator,
        creator_url=clean_image.creator_url,
        title=clean_image.title,
        meta_data=clean_image.meta_data,
        tags=clean_image.tags,
        watermarked=clean_image.watermarked,
    )

    return clean_row

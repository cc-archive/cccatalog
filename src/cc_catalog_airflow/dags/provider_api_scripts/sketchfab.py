"""
Content Provider:  Sketchfab

ETL Process:       Use the API to identify all CC licensed images.

Output:            TSV file containing the images and the respective
                   meta-data.

Notes:             https://docs.sketchfab.com/data-api/v3/index.html
"""

import argparse
from datetime import datetime, timedelta, timezone
import logging

from common.requester import DelayedRequester
from common.storage import image

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.INFO
)

logger = logging.getLogger(__name__)

DELAY = 1.0
LIMIT = 24  # This appears to be the max results per request allowed
MAX_TAG_STRING_LENGTH = 2000
MAX_DESCRIPTION_LENGTH = 2000
PROVIDER = 'sketchfab'
ENDPOINT_STUB = 'https://api.sketchfab.com/v3/'
LICENSE_ENDPOINT = ENDPOINT_STUB + 'licenses'
SEARCH_ENDPOINT = ENDPOINT_STUB + 'search'

DEFAULT_QUERY_PARAMS = {
    'count': LIMIT,
    'sort_by': '-viewCount',
    'type': 'models',
}

delayed_requester = DelayedRequester(DELAY)
image_store = image.ImageStore(provider=PROVIDER)


def main():
    logger.info('Processing Sketchfab API')
    cc_licenses = _get_license_dict()
    for license_tuple in cc_licenses:
        logger.info(f'processing license tuple {license_tuple}')
        _process_license(*license_tuple)
    total_images = image_store.commit()
    logger.info(f'Total images: {total_images}')
    logger.info('Terminated!')


def _url_join(*args):
    return '/'.join(
        [s.strip('/') for s in args]
    )


def _get_license_dict(license_endpoint=LICENSE_ENDPOINT):
    license_response = delayed_requester.get(license_endpoint)
    # If we fail here, we should FAIL so that Airflow knows about it.
    license_dict = license_response.json()['results']
    return [
        (i['slug'], i['url']) for i in license_dict
        if i['url'] is not None and 'creativecommons.org' in i['url']
    ]


def _process_license(
        license_slug,
        license_url
):
    cursor = 0
    new_cursor = None
    while True:
        logger.info(f'Getting search_result_list. Cursor: {cursor}')
        result_list, new_cursor = _get_search_result_list(
            license_slug,
            cursor=cursor
        )
        if new_cursor == cursor:
            break
        else:
            cursor = new_cursor
        total_images = _process_result_list(result_list, license_url)
        logger.info(f'Total Images so far: {total_images}')


def _get_search_result_list(
        license_slug,
        cursor=0,
        endpoint=SEARCH_ENDPOINT,
        attempts=5
):
    if attempts == 0:
        logger.warning('No attempts remaining.  Returning Nonetypes.')
        return None, None
    else:
        query_param_dict = _build_search_query_param_dict(license_slug, cursor)
        response = delayed_requester.get(endpoint, params=query_param_dict)
    logger.debug('response.status_code: {response.status_code}')
    result_list, new_cursor = _extract_result_list_from_response(response)
    if result_list is None or new_cursor is None:
        result_list, new_cursor = _get_search_result_list(
            license_slug,
            cursor,
            endpoint=endpoint,
            attempts=attempts - 1
        )

    return result_list, new_cursor


def _build_search_query_param_dict(
        license_slug,
        cursor,
        base_params=DEFAULT_QUERY_PARAMS
):
    query_params = base_params.copy()
    query_params.update(
        {
            'license': license_slug,
            'cursor': cursor
        }
    )
    return query_params


def _extract_result_list_from_response(response):
    if response is not None and response.status_code == 200:
        try:
            response_json = response.json()
        except Exception as e:
            logger.warning(f'Could not get response json.\n{e}')
            response_json = None
    else:
        response_json = None

    result_list = response_json.get('results')
    new_cursor = response_json.get('cursors', {}).get('next')
    return result_list, new_cursor


def _process_result_list(result_list, license_url):
    for result in result_list:
        if result.get('isAgeRestricted'):
            continue
        image_url, thumbnail, height, width = _get_image_data(result)
        total_images = image_store.add_item(
            foreign_landing_url=result.get('viewerUrl'),
            image_url=image_url,
            thumbnail_url=thumbnail if thumbnail is not None else image_url,
            license_url=license_url,
            license_=None,
            license_version=None,
            foreign_identifier=result.get('uid'),
            width=width,
            height=height,
            creator=result.get('user', {}).get('displayName'),
            creator_url=result.get('user', {}).get('profileUrl'),
            title=result.get('name'),
            meta_data=_get_metadata_dict(result),
            raw_tags=_get_raw_tags(result)
        )
    return total_images


def _get_image_data(result):
    image_url, thumbnail, height, width = None, None, None, None
    image_list = result.get('thumbnails', {}).get('images')
    if image_list is not None:
        image_data = _choose_max_width_image_data(image_list)
        image_url = image_data.get('url')
        height = image_data.get('height')
        width = image_data.get('width')
        thumbnail = _choose_thumbnail(image_list)
    else:
        logger.warning(f'No image_list found in\n{result}')

    return image_url, thumbnail, height, width


def _choose_max_width_image_data(image_list):
    return max(
        image_list,
        key=lambda i: i.get('width', 0)
    )


def _choose_thumbnail(image_list):
    potential_thumbnails = [
        i for i in image_list
        if i.get('width', 0) > 250 and i.get('width') < 350
    ]
    if potential_thumbnails:
        return _choose_max_width_image_data(potential_thumbnails).get('url')


def _get_metadata_dict(result):
    return {
        'views': result.get('viewCount'),
        'likes': result.get('likeCount'),
        'comments': result.get('commentCount'),
        'description': result.get('description'),
    }


def _get_raw_tags(result):
    return [t.get('name') for t in result.get('tags', [])]

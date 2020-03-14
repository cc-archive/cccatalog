"""
Content Provider:       Internet Archivee

ETL Process:            Use the API to identify all CC-licensed audio.

Output:                 TSV file containing the audio, the respective
                        meta-data.

Notes:                  https://blog.archive.org/developers/
                        No rate limit specified.
"""

import argparse
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import logging
import os
from urllib.parse import urlparse

import lxml.html as html

import common.requester as requester
import common.storage.image as image

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.INFO
)

logger = logging.getLogger(__name__)

ROWS_PER_PAGE = 200
# The number is chosen at random corresponds
# to the number of entries returned on each
# page.
DELAY = 0
HOST = 'archive.org'
ENDPOINT = f'https://{HOST}/advancedsearch.php'
PROVIDER = 'internet_archive'
UA_STRING = (
    f'CC-Catalog/0.1 (https://creativecommons.org;)'
)
DEFAULT_REQUEST_HEADERS = {
    'User-Agent': UA_STRING
}
DEFAULT_QUERY_PARAMS = {
    'q': 'mediatype:audio',
    'fl[]': 'identifier,title,mediatype,collection,licenseurl,date',
    'rows': str(ROWS_PER_PAGE),
    'output': 'json',
}

DETAIL_URL = 'https://archive.org/details/{}'

META_URL = 'https://archive.org/metadata/{}'

DOWNLOAD_URL = 'https://archive.org/download/{}/{}'

delayed_requester = requester.DelayedRequester(DELAY)
# The image_store is used for now and will be changed to the audio
# specific class once the database is ready
image_store = image.ImageStore(provider=PROVIDER)


def main(date):
    """
    This script pulls the data for a given date from the Internet
    ARCHIVE API, and writes it into a .TSV file to be eventually read
    into the DB.

    Required Arguments:

    date:  Date String in the form YYYY-MM-DD.  This is the date for
           which running the script will pull data.

    """

    logger.info(f'Processing Internet Archive API for date: {date}')

    start_timestamp, end_timestamp = _derive_timestamp_pair(date)
    total_sounds = _process_pages(start_timestamp, end_timestamp)
    total_sounds = image_store.commit()

    logger.info(f'Total sounds: {total_sounds}')
    logger.info('Terminated!')


def _process_pages(start_timestamp, end_timestamp):
    page = 1
    total_pages = _get_total_pages(start_timestamp, end_timestamp)
    for page in range (1,total_pages+1):
        sounds_for_page = _get_sound_for_page(
            start_timestamp,
            end_timestamp,
            total_pages,
            page)
        if sounds_for_page is not None:
            total_sounds = _process_page(sounds_for_page)
            logger.info(f'Total sound processed so far: {total_sounds}')
        else:
            logger.warning('No sound data!  Attempting to continue')

    logger.info(f'Total page processed: {page-1}')
    return total_sounds


def _derive_timestamp_pair(date):
    date_obj = datetime.strptime(date, '%Y-%m-%d')
    utc_date = date_obj.replace(tzinfo=timezone.utc)
    start_timestamp = utc_date
    end_timestamp = utc_date + timedelta(days=1)
    return str(start_timestamp.date()), \
        str(end_timestamp.date())


def _get_sound_for_page(
    start_timestamp,
    end_timestamp,
    page=1,
    retries=5
):
    query_params = _build_query_params(
        start_timestamp,
        end_timestamp,
        page
    )

    sounds_for_page = _get_response_json(
        query_params,
        endpoint=ENDPOINT,
        request_headers=DEFAULT_QUERY_PARAMS,
        retries=retries
    ).get('response')['docs']

    return sounds_for_page


def _get_total_pages(start_timestamp, end_timestamp):
    query_params = _build_query_params(
        start_timestamp,
        end_timestamp,
        1
    )

    total_pages = int(_get_response_json(
        query_params,
        endpoint=ENDPOINT,
        request_headers=DEFAULT_QUERY_PARAMS,
        retries=0
    ).get('response')['numFound']/ROWS_PER_PAGE)
    logger.info(f'Total pages: {total_pages}')

    return (total_pages)


def _build_query_params(
        start_date,
        end_date,
        page,
        default_query_params=DEFAULT_QUERY_PARAMS,
):
    query_params = default_query_params.copy()
    query_params['q'] += ' AND date:[' + start_date + ' TO ' + end_date + "]"
    query_params.update({'page': page})
    return query_params


def _get_response_json(
        query_params,
        endpoint=ENDPOINT,
        request_headers=DEFAULT_REQUEST_HEADERS,
        retries=0,
):

    response_json = None

    if retries < 0:
        logger.error('No retries remaining.  Failure.')
        raise Exception('Retries exceeded')

    response = delayed_requester.get(
        endpoint,
        params=query_params,
        headers=request_headers,
        timeout=60
    )
    if response is not None and response.status_code == 200:
        try:
            response_json = response.json()
        except Exception as e:
            logger.warning(f'Could not get response_json.\n{e}')
            response_json = None

    if (
            response_json is None
            or response_json.get('error') is not None
    ):
        logger.warning(f'Bad response_json:  {response_json}')
        logger.warning(
            'Retrying:\n_get_response_json(\n'
            f'    {endpoint},\n'
            f'    {query_params},\n'
            f'    {request_headers}'
            f'    retries={retries - 1}'
            ')'
        )
        response_json = _get_response_json(
            query_params,
            endpoint=endpoint,
            request_headers=request_headers,
            retries=retries - 1
        )

    return response_json


def _process_page(sounds_for_page):
    for i in sounds_for_page:
        if i.get('licenseurl'):
            sounds_for_page = _process_sound(i)

    return sounds_for_page


def _process_sound(sound):

    return image_store.add_item(
        foreign_landing_url=DETAIL_URL.format(sound.get('identifier')),
        license_url=sound.get('licenseurl'),
        foreign_identifier=sound.get('identifier'),
        creator=sound.get('creator'),
        title=sound.get('title'),
        image_url=_get_download_url(sound.get('identifier')),
        meta_data=_get_meta_data(sound.get('identifier')),
    )


def _get_meta_data(identifier):
    meta_data = delayed_requester.get(META_URL.format(identifier))
    return meta_data.json()


def _get_download_url(identifier):
    meta_data = delayed_requester.get(META_URL.format(identifier)).json()
    for i in meta_data.get('files'):
        if ".mp3" in i.get('name'):
            return (DOWNLOAD_URL.format(identifier, i.get('name')))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Internet Archive API Job',
        add_help=True,
    )
    parser.add_argument(
        '--date',
        help='Identify images uploaded on a date (format: YYYY-MM-DD).')
    args = parser.parse_args()
    if args.date:
        date = args.date
    else:
        date_obj = datetime.now() - timedelta(days=2)
        date = datetime.strftime(date_obj, '%Y-%m-%d')

    main(date)

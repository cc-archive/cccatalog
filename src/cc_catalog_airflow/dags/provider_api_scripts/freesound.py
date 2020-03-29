"""
Content Provider:       Freesound

ETL Process:            Use the API to identify all CC licensed images.

Output:                 TSV file containing the images and the
                        respective meta-data.

Notes:                  https://freesound.org/docs/api/overview.html
                        Rate limit: 60 requests / minute, 2000 requests / day.
"""

import argparse
from datetime import datetime, timedelta
import logging
import os

from common.requester import DelayedRequester
from common.storage import audio

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.INFO
)

logger = logging.getLogger(__name__)

SECONDS_IN_DAY = 60 * 60 * 24
FREESOUND_LIMIT = 2000
DELAY = 1.0  # SECONDS_IN_DAY / FREESOUND_LIMIT
LIMIT = 150
PROVIDER = 'freesound'
API_KEY = os.getenv('FREESOUND_API_KEY')
ENDPOINT = 'https://freesound.org/apiv2/search/text/'
MAX_TAG_STRING_LENGTH = 2000

DEFAULT_QUERY_PARAMS = {
    'format': 'json',
    'fields': ('id,url,download,license,name,pack,username,type,duration,'
               'samplerate,bitdepth,channels,description,tags'),
}

delayed_requester = DelayedRequester(DELAY)
audio_store = audio.AudioStore(provider=PROVIDER)


def main(date):
    logger.info(f'Processing Freesound API for date: {date}')

    start_timestamp, end_timestamp = _derive_timestamp_pair(date)

    total_sounds = _process_date(start_timestamp, end_timestamp)

    total_sounds = audio_store.commit()
    logger.info(f'Total sounds: {total_sounds}')
    logger.info('Terminated!')


def _derive_timestamp_pair(date):
    datetime.strptime(date, '%Y-%m-%d')  # validate date
    return (date + 'T00:00:00.000Z',
            date + 'T23:59:59.999Z')


def _process_date(start_timestamp, end_timestamp):
    has_next_page = True
    page_number = 1
    total_sounds = 0

    while has_next_page:
        logger.info(f'Processing page: {page_number}')

        sound_list, next_page = _get_sound_list(
            start_timestamp,
            end_timestamp,
            page_number,
        )

        if sound_list is not None:
            total_sounds = _process_sound_list(sound_list)
            logger.info(f'Total sounds so far: {total_sounds}')
        else:
            logger.warning('No sound data!  Attempting to continue')

        has_next_page = next_page is not None

        page_number += 1

    logger.info(f'Total pages processed: {page_number}')

    return total_sounds


def _get_sound_list(
        start_timestamp,
        end_timestamp,
        page_number,
        endpoint=ENDPOINT,
        max_tries=6  # one original try, plus 5 retries
):
    for try_number in range(max_tries):
        query_param_dict = _build_query_param_dict(
            start_timestamp,
            end_timestamp,
            page_number,
        )
        response = delayed_requester.get(
            endpoint,
            params=query_param_dict,
        )

        logger.debug('response.status_code: {response.status_code}')
        response_json = _extract_response_json(response)
        sound_list, next_page = _extract_sound_list_from_json(response_json)

    if try_number == max_tries - 1 and sound_list is None:
        logger.warning('No more tries remaining. Returning Nonetypes.')
        return None, None
    else:
        return sound_list, next_page


def _extract_response_json(response):
    if response is not None and response.status_code == 200:
        try:
            response_json = response.json()
        except Exception as e:
            logger.warning(f'Could not get sound_data json.\n{e}')
            response_json = None
    else:
        response_json = None

    return response_json


def _build_query_param_dict(
        start_timestamp,
        end_timestamp,
        cur_page,
        api_key=API_KEY,
        limit=LIMIT,
        default_query_param=DEFAULT_QUERY_PARAMS,
):
    query_param_dict = default_query_param.copy()
    query_param_dict.update(
        {
            'filter': f'created:[{start_timestamp} TO {end_timestamp}]',
            'page': cur_page,
            'token': api_key,
            'page_size': limit,
        }
    )

    return query_param_dict


def _extract_sound_list_from_json(response_json):
    if (response_json is None):
        sound_list = None
        next_page = None
    else:
        sound_list = response_json.get('results')
        next_page = response_json.get('next')

    return sound_list, next_page


def _process_sound_list(sound_list):
    for sound_data in sound_list:
        total_sounds = _process_sound_data(sound_data)

    return total_sounds


def _process_sound_data(sound_data):
    logger.debug(f'Processing sound data: {sound_data}')

    return audio_store.add_item(
        foreign_landing_url=sound_data.get('url'),
        audio_url=sound_data.get('download'),
        file_format=sound_data.get('type'),
        duration=sound_data.get('duration'),
        samplerate=sound_data.get('samplerate'),
        bitdepth=sound_data.get('bitdepth'),
        channels=sound_data.get('channels'),
        license_url=sound_data.get('license'),
        creator=sound_data.get('username'),
        title=sound_data.get('name'),
        album=sound_data.get('pack'),
        meta_data={
            'description': sound_data.get('description'),
        },
        raw_tags=_create_tags_list(sound_data),
    )


def _create_tags_list(
        sound_data,
        max_tag_string_length=MAX_TAG_STRING_LENGTH
):
    raw_tags = None
    # We limit the input tag string length, not the number of tags,
    # since tags could otherwise be arbitrarily long, resulting in
    # arbitrarily large data in the DB.
    raw_tag_string = ' '.join(sound_data.get('tags', [])) \
        .strip()[:max_tag_string_length]
    if raw_tag_string:
        # We sort for further consistency between runs, saving on
        # inserts into the DB later.
        raw_tags = sorted(list(set(raw_tag_string.split())))

    return raw_tags


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Freesound API Job',
        add_help=True
    )
    parser.add_argument(
        '--date',
        help='Identify sounds uploaded on a date (format: YYYY-MM-DD).')
    args = parser.parse_args()
    if args.date:
        date = args.date
    else:
        date_obj = datetime.now() - timedelta(days=2)
        date = datetime.strftime(date_obj, '%Y-%m-%d')

    main(date)

"""
Content Provider:       Thingiverse
ETL Process:            Use the API to identify all CC0 3D Models.
Output:                 TSV file containing the 3D models
                        their respective images and meta-data.
Notes:                  https://www.thingiverse.com/developers/getting-started
                        All API requests require authentication.
                        Rate limiting is 300 per 5 minute window.
"""
import logging
import os
from datetime import datetime, timedelta, timezone

import common.requester as requester
import common.storage.image as image_class

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.INFO
)

logger = logging.getLogger(__name__)

LIMIT = 30  # Number of images to pull at a time
DELAY = 1.0  # Time between each two consecutive requets
HOST = 'thingiverse.com'
ENDPOINT = f'https://api.{HOST}/newest'
PROVIDER = 'thingiverse'
LICENSE = 'CC0'
LICENSE_VERSION = '1.0'
TOKEN = os.getenv('THINGIVERSE_TOKEN')
DEFAULT_QUERY_PARAMS = {
    'access_token': TOKEN,
}

delayed_requester = requester.DelayedRequester(DELAY)
image_store = image_class.ImageStore(provider=PROVIDER)


def main(date):
    """
    This script pulls the data for a given date from Thingiverse API
    and writes it into a .TSV file to be eventually read
    into our DB.
    Required Arguments:
    date:  Date String in the form YYYY-MM-DD.  This is the date for
           which running the script will pull data.
    """
    logger.info(f'Processing Thingiverse API for date: {date}')

    start_timestamp, end_timestamp = _derive_timestamp_pair(date)

    cur_page = 1
    total_images = 0
    is_valid = True

    while is_valid:
        thing_batch = _get_things_batch(
            start_timestamp,
            end_timestamp,
            cur_page
        )
        if thing_batch is not None:
            for thing in thing_batch:
                total_images = _process_thing(
                    thing, start_timestamp, end_timestamp)
                if total_images == 0:
                    is_valid = False
                    break

        cur_page = cur_page + 1

    total_images = image_store.commit()
    logger.info(f'Total images: {total_images}')
    logging.info('Terminated!')


def _derive_timestamp_pair(date):
    date_obj = datetime.strptime(date, '%Y-%m-%d')
    utc_date = date_obj.replace(tzinfo=timezone.utc)
    start_timestamp = str(int(utc_date.timestamp()))
    end_timestamp = str(int((utc_date + timedelta(days=1)).timestamp()))
    return start_timestamp, end_timestamp


def _get_things_batch(start_timestamp, end_timestamp, page=1, retries=5):
    query_params = _build_query_params(
        start_timestamp,
        end_timestamp,
        page
    )
    thing_batch = None
    response_json = _get_response_json(query_params, retries=retries)
    if response_json is not None:
        thing_batch = response_json

    if 'batchcomplete' in response_json:
        logger.debug('Found batchcomplete')

    return thing_batch


def _build_query_params(
        page=1,
        default_query_params=DEFAULT_QUERY_PARAMS,
        thing=True
):
    query_params = default_query_params.copy()

    per_page = LIMIT
    query_params.update(per_page)

    query_params.update(page)

    return query_params


def _get_response_json(
    query_params,
    endpoint=ENDPOINT,
    retries=0
):
    response_json = None

    if retries < 0:
        logger.error('No retries remaining.  Failure.')
        raise Exception('Retries exceeded')

    response = delayed_requester.get(
        endpoint,
        params=query_params,
        timeout=60
    )
    if response is not None and response.status_code == 200:
        try:
            response_json = response.json()
        except Exception as e:
            logger.warning(f'Could not get response_json.\n{e}')
            response_json = None

    if(
        response_json is None
        or
        response_json.get('error') is not None
    ):
        logger.warning(f'Bad response_json:  {response_json}')
        logger.warning(
            'Retrying:\n_get_response_json(\n'
            f'    {endpoint},\n'
            f'    {query_params},\n'
            f'    retries={retries - 1}'
            ')'
        )
        response_json = _get_response_json(
            query_params,
            endpoint=endpoint,
            retries=retries - 1
        )

    return response_json


def _build_thing_query(thing, default_query_params=DEFAULT_QUERY_PARAMS):
    endpoint = f'https://api.thingiverse.com/things/{thing}'
    query_params = default_query_params.copy()
    return endpoint, query_params


def _validate_license(response_json):
    license_text = 'Creative Commons - Public Domain Dedication'
    if not (('license' in response_json) and (
            license_text.lower() in response_json['license'].lower())):
        logging.warning('license not detected')
        license_ = None
        license_version = None
    else:
        license_ = LICENSE
        license_version = LICENSE_VERSION

    return license_, license_version


def _create_meta_dict(
    thing_data
):
    meta_data = {
        'description': thing_data.get('description', ''),
        'title': thing_data.get('name', '')
    }

    return meta_data


def _build_foreign_landing_url(thing_data, thing):
    foreign_landing_url = None
    if 'public_url' in thing_data:
        foreign_landing_url = thing_data['public_url'].strip()
    else:
        foreign_landing_url = 'https://www.thingiverse.com/thing:{}'.format(
            thing)

    return foreign_landing_url


def _build_creator_data(thing_data):
    creator = None
    creator_url = None
    if 'creator' in thing_data:
        if ('first_name' in thing_data['creator']) and (
                'last_name' in thing_data['creator']):
            creator = '{} {}'.format(
                thing_data['creator']['first_name'],
                thing_data['creator']['last_name'])

        if (creator.strip() == '') and ('name' in thing_data['creator']):
            creator = thing_data['creator']['name']

        if 'public_url' in thing_data['creator']:
            creator_url = thing_data['creator']['public_url'].strip()

    return creator, creator_url


def _create_tags_list(thing, endpoint):
    logging.info('Requesting tags for thing: {}'.format(thing))
    tags = _get_response_json({}, endpoint, retries=5)
    tags_list = None
    if tags is not None:
        tags_list = sorted(list(set(tags.split())))

    return tags_list


def _get_image_list_json(thing, endpoint):
    endpoint.replace(thing, '{}/files'.format(thing))
    image_list = _get_response_json(DEFAULT_QUERY_PARAMS, endpoint, retries=5)
    if image_list is None:
        logging.warning('Image Not Detected!')

    return image_list


def _get_image_list_meta_data(image, thing_meta_data):
    meta_data = {}
    meta_data['description'] = thing_meta_data['description']
    if ('default_image' in image) and image['default_image']:
        if 'url' in image['default_image']:
            meta_data['3d_model'] = image['default_image']['url']

    return meta_data


def _get_image_url_thumbnail(images):
    thumbnail = None
    image_url = None

    for image_size in images:

        if str(image_size['type']).strip().lower() == 'display':

            if str(image_size['size']).lower() == 'medium':
                thumbnail = image_size['url'].strip()

            if str(image_size['size']).lower() == 'large':
                image_url = image_size['url'].strip()

            elif image_url is None:
                image_url = thumbnail

        else:
            continue

    return thumbnail, image_url


def _add_image_item(
        image_url,
        foreign_landing_url,
        foreign_identifier,
        thumbnail_url,
        license_,
        license_version,
        creator,
        creator_url,
        title,
        meta_data,
        raw_tags):
    return image_store._add_item(
        foreign_landing_url=foreign_landing_url,
        image_url=image_url,
        thumbnail_url=thumbnail_url,
        license_=license_,
        license_version=license_version,
        foreign_identifier=foreign_identifier,
        creator=creator,
        creator_url=creator_url,
        title=title,
        meta_data=meta_data,
        raw_tags=raw_tags
    )


def _get_image_list(
        thing,
        endpoint,
        thing_meta_data,
        foreign_landing_url,
        license_,
        license_version,
        creator,
        creator_url,
        title,
        tags_list):
    logging.info('Requesting images for thing: {}'.format(thing))
    total_images = 0
    image_list = _get_image_list_json(thing, endpoint)
    if image_list is not None:
        for image in image_list:
            meta_data = _get_image_list_meta_data(image, thing_meta_data)
            foreign_landing_id = str(image['default_image']['id'])
            images = image['default_image']['sizes']

            thumbnail, image_url = _get_image_url_thumbnail(images)
            if image_url is None:
                logging.warning('Image Not Detected!')
                continue

            total_images = _add_image_item(
                image_url,
                foreign_landing_url,
                foreign_landing_id,
                thumbnail,
                license_,
                license_version,
                creator,
                creator_url,
                title,
                meta_data,
                tags_list)

    return total_images


def _process_thing(thing, start_timestamp, end_timestamp):
    license_ = None
    license_version = None

    endpoint, query_params = _build_thing_query(thing)
    response_json = _get_response_json(query_params, endpoint, retries=5)
    if response_json is not None:
        license_, license_version = _validate_license(response_json)
        meta_data = _create_meta_dict(response_json)
        foreign_landing_url = _build_foreign_landing_url(response_json, thing)
        creator, creator_url = _build_creator_data(response_json)
        tags_list = _create_tags_list(thing, endpoint)
        total_images = _get_image_list(
            thing,
            endpoint,
            meta_data,
            foreign_landing_url,
            license_,
            license_version,
            creator,
            creator_url,
            meta_data['title'],
            tags_list)
        return total_images

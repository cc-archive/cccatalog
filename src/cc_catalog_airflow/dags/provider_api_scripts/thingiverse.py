"""
Content Provider:       Thingiverse
ETL Process:            Use the API to identify all CC0 3D Models.
Output:                 TSV file containing the 3D models
                        their respective images and meta-data.
Notes:                  https://www.thingiverse.com/developers/getting-started
                        All API requests require authentication.
                        Rate limiting is 300 per 5 minute window.
"""
import argparse
import logging
import os
import json
from operator import itemgetter
from datetime import datetime, timedelta, timezone

import common.requester as requester
import common.storage.image as image_class

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.INFO
)

logger = logging.getLogger(__name__)

PER_PAGE_LIMIT = 30
IMAGES_LIMIT = 1000
DELAY = 1.0  # Time between each two consecutive requets
HOST = 'thingiverse.com'
ENDPOINT = f'https://api.{HOST}/newest'
PROVIDER = 'thingiverse'
LICENSE = 'cc0'
LICENSE_VERSION = '1.0'
LICENSE_TEXT = 'Creative Commons'
TOKEN = ''
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

    cur_page = 20000
    total_images = 0
    is_valid = True

    while is_valid and total_images < IMAGES_LIMIT:
        thing_batch = _get_things_batch(
            cur_page
        )
        total_images, is_valid = _process_thing_batch(
            thing_batch, total_images, start_timestamp, end_timestamp)
        logger.info(f'Total images: {total_images}')
        cur_page = cur_page + 1

    total_images = image_store.commit()
    logger.info(f'Total images: {total_images}')
    logging.info('Terminated!')


def _process_thing_batch(thing_batch, total_images, start_timestamp, end_timestamp):
    is_valid = True
    if thing_batch is not None:
        thing_batch = list(thing_batch)
        batch_total_images = list(filter(None, list(map(lambda thing: _process_thing(
            str(thing), start_timestamp, end_timestamp), thing_batch))))

        if '-1' in batch_total_images:
            is_valid = False
            batch_total_images = batch_total_images.remove('-1')

        if batch_total_images != 0:
            total_images += sum(batch_total_images)

    return total_images, is_valid


def _derive_timestamp_pair(date):
    date_obj = datetime.strptime(date, '%Y-%m-%d')
    utc_date = date_obj.replace(tzinfo=timezone.utc)
    start_timestamp = str(int(utc_date.timestamp()))
    end_timestamp = str(int((utc_date + timedelta(days=1)).timestamp()))
    return start_timestamp, end_timestamp


def _get_things_batch(page=1, retries=5):
    query_params = _build_query_params(
        page
    )
    thing_batch = None
    response_json = _get_response_json_list(
        query_params=query_params, retries=retries)
    if response_json is not None:
        thing_batch = map(lambda thing: thing['id'], response_json)

    if 'batchcomplete' in response_json:
        logger.debug('Found batchcomplete')

    return thing_batch


def _build_query_params(
        page=1,
        default_query_params=DEFAULT_QUERY_PARAMS
):
    query_params = default_query_params.copy()
    query_params.update({'per_page': PER_PAGE_LIMIT})
    query_params.update({'page': page})

    return query_params


def _get_response_json(
    query_params,
    endpoint=ENDPOINT,
    retries=5
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


def _get_response_json_list(
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
    if response_json is None or (type(response_json) != list and response_json.get('error') is not None):
        logger.warning(f'Bad response_json:  {response_json}')
        logger.warning(
            'Retrying:\n_get_response_json(\n'
            f'    {endpoint},\n'
            f'    {query_params},\n'
            f'    retries={retries - 1}'
            ')'
        )
        response_json = _get_response_json_list(
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
    license_ = None
    license_version = None
    if not (('license' in response_json) and (
            LICENSE_TEXT.lower() in response_json['license'].lower())):
        logging.warning('license not detected')
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
        foreign_landing_url = f'https://www.thingiverse.com/thing:{thing}'

    return foreign_landing_url


def _build_creator_data(thing_data):
    creator = None
    creator_url = None
    try:
        if ('first_name' in thing_data['creator']) and (
                'last_name' in thing_data['creator']):
            creator = f"{thing_data['creator']['first_name']} {thing_data['creator']['last_name']}"

        if (creator.strip() == '') and ('name' in thing_data['creator']):
            creator = thing_data['creator']['name']

        if 'public_url' in thing_data['creator']:
            creator_url = thing_data['creator']['public_url'].strip()

    except Exception as e:
        logger.warning(f'Could not find "creator" in "thing_data". \n {e}')

    return creator, creator_url


def _create_tags_list(thing):
    logging.info(f'Requesting tags for thing: {thing}')
    endpoint = f'https://api.thingiverse.com/things/{thing}/tags'
    tags = _get_response_json_list(DEFAULT_QUERY_PARAMS, endpoint, retries=5)
    tags_list = None
    if tags is not None:
        tags_list = list(map(lambda tag: {'name': str(
            tag['name'].strip()), 'provider': 'thingiverse'}, tags))
        tags_list.sort(key=itemgetter('name'))
    return tags_list


def _get_image_list_json(thing):
    endpoint = f'https://api.thingiverse.com/things/{thing}/files'
    image_list = _get_response_json_list(
        DEFAULT_QUERY_PARAMS, endpoint, retries=5)
    if image_list is None:
        logging.warning('Image Not Detected!')

    return image_list


def _get_image_meta_data(image, thing_meta_data):
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


def _add_images(image_list,
                foreign_landing_url,
                license_,
                license_version,
                creator,
                creator_url,
                title,
                tags_list):
    total_images = 0
    for image in image_list:
        total_images = image_store.add_item(
            foreign_landing_url=foreign_landing_url,
            image_url=image[1],
            thumbnail_url=image[2],
            license_=license_,
            license_version=license_version,
            foreign_identifier=image[0],
            creator=creator,
            creator_url=creator_url,
            title=title,
            meta_data=image[3],
            raw_tags=tags_list,
            source='Thigniverse'
        )
    return total_images


def _process_image_list(image_list, description):
    print("Hello from image list")
    print("this is the image list I recieved")
    print(image_list)
    images_data = []
    for image in image_list:
        print("Hello from image list loop")
        meta_data = {}
        thumbnail = None
        image_url = None
        foreign_landing_id = None
        meta_data['description'] = description
        print('DESCRIPTION: ')
        print(description)
        if ('default_image' in image) and image['default_image']:
            if 'url' in image['default_image']:
                meta_data['3d_model'] = image['default_image']['url']
                foreign_landing_id = str(image['default_image']['id'])
                images = image['default_image']['sizes']
                print("3d_model:")
                print(meta_data['3d_model'])
                print('foreign_landing_id: ')
                print(foreign_landing_id)
                thumbnail, image_url = _get_image_url_thumbnail(images)
                print('thumbnail: ')
                print(thumbnail)
                print('image_url: ')
                print(image_url)
                if image_url is None:
                    logging.warning('Image Not Detected!')
                    continue

                images_data.append(
                    [
                        image_url if not foreign_landing_id else foreign_landing_id,
                        image_url,
                        thumbnail,
                        '\\N' if not meta_data else json.dumps(meta_data)
                    ]
                )
            else:
                logging.warning('3D Model Not Detected!')
                continue
        else:
            logging.warning('Not valid image!')

    print(images_data)
    return images_data


def _process_thing(thing, start_timestamp, end_timestamp):
    endpoint, query_params = _build_thing_query(thing)
    license_ = None
    license_version = None
    creator = None
    creator_url = None
    foreign_landing_url = None
    total_images = 0
    total_images_list = []
    logging.info('Processing thing: {}'.format(thing))
    response_json = _get_response_json_list(query_params, endpoint, retries=5)

    if response_json is not None:
        modified_date = response_json.get('modified', '')
        if modified_date is not None:
            modified_date = modified_date.split('T')[0].strip()
            modified_date, _ = _derive_timestamp_pair(modified_date)
            if modified_date >= start_timestamp or modified_date <= end_timestamp:
                license_, license_version = _validate_license(response_json)
                meta_data = _create_meta_dict(response_json)
                foreign_landing_url = _build_foreign_landing_url(
                    response_json, thing)
                creator, creator_url = _build_creator_data(response_json)
                tags_list = _create_tags_list(str(thing))
                image_list = _get_image_list_json(thing)
                print("THIS IS THE IMAGE LIST BEFORE PROCESSING")
                print(image_list)
                total_images_list = _process_image_list(
                    image_list, meta_data['description'])
                print("THIS IS IMAGE LIST AFTER PROCCESSING BEFORE ADD ITEM")
                print(total_images_list)
                total_images = _add_images(total_images_list, foreign_landing_url, license_,
                                           license_version, creator, creator_url, meta_data['title'], tags_list)
                print("TOTAL IMAGES AFTER ADD ITEM")
                print(total_images)
            else:
                total_images = '-1'

    return total_images


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Thingiverse API Job',
        add_help=True
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

import argparse
from datetime import datetime, timedelta, timezone
import logging
import os
import requests
import common.requester as requester
import common.storage.image as image_class
import json  # It's temporary make sure to remove it.
LIMIT = 500   # It sets the limit to how many images we want to pull at a time
DELAY = 5.0   # Time Delay between consecutive API requests(in seconds)
MEAN_GLOBAL_USAGE_LIMIT = 1000
PROVIDER = 'Cleveland Museum of Art'


logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.INFO
)

logger = logging.getLogger(__name__)


DEFAULT_QUERY_PARAMS = {
    'cc0': '1',
    'has_image': '1'
}

CONTACT_EMAIL = os.getenv('WM_SCRIPT_CONTACT')
UA_STRING = (
    f'CC-Catalog/0.1 (https://creativecommons.org; {CONTACT_EMAIL})'
)
DEFAULT_REQUEST_HEADERS = {
    'User-Agent': UA_STRING
}


delayed_requester = requester.DelayedRequester(DELAY)
image_store = image_class.ImageStore(provider=PROVIDER)


def main(time):
    logger.info(f'Starting Cleveland API requets for date :{date}')
    # start_time = time.time()  # Denotes the starting time of hitting API.
    start_timestamp, end_timestamp = _derive_timestamp_pair(date)
    query_params = _build_query_params(start_timestamp, end_timestamp)
    condition = True
    offset = 0
    while condition:
        endpoint = 'https://openaccess-api.clevelandart.org/api/artworks/?cc0=1&skip={0}&limit={1}&indent=1&has_image=1'.format(offset, LIMIT)
        response = _get_response_json(query_params, endpoint)
        if response is not None and ('data' in response):
            batch = response['data']
            images_till_now = handle_the_response(batch)
            logger.info(f'Total Images till now {images_till_now}')
            offset = offset + LIMIT
        else:
            logger.info(f'No more images to process so exiting the loop')
            condition = False
    total_images = image_store.commit()
    logger.info(f'Total number of images received {total_images}')



def _build_query_params(
        start_date,
        end_date,
        default_query_params=DEFAULT_QUERY_PARAMS,
):
    query_params = default_query_params.copy()
    query_params['gaistart'] = start_date
    query_params['gaiend'] = end_date
    return query_params


def _derive_timestamp_pair(date):
    date_obj = datetime.strptime(date, '%Y-%m-%d')
    utc_date = date_obj.replace(tzinfo=timezone.utc)
    start_timestamp = str(int(utc_date.timestamp()))
    end_timestamp = str(int((utc_date + timedelta(days=1)).timestamp()))
    return start_timestamp, end_timestamp


def _get_response_json(
        query_params,
        endpoint,
        request_headers=DEFAULT_REQUEST_HEADERS,
        retries=5,
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




def handle_the_response(response):
    for data in response:
        foreign_url = data.get('url', None)
        single_image = data.get('images')
        image_url = None
        images_till_now = 0
        if single_image is not None:
            if single_image.get('web'):
                key = 'web'
                image_url = single_image.get('web').get('url', None)
            elif single_image.get('print'):
                key = 'print'
                image_url = single_image.get('print').get('url', None)
            else:
                key = 'full'
                image_url = single_image.get('full').get('url', None)
            
            width = single_image[key]['width']
            height = single_image[key]['height']
            foreign_id = data.get('id', image_url)
            license_status = data.get('share_license_status', None).lower()
            license_share = license_status.lower()
            title = data.get('title', '')

            creator_info = data.get('creators', {})
            creator_name = None
            if creator_info:
                creator_name = creator_info[0].get('description', '')
            meta_data = create_meta_data(data)
            images_till_now = image_store.add_item(
                foreign_url,  # Foreign Landing URl
                image_url,  # Image URL
                None,  # Thumbnail_URL
                None,  # License_URL
                license_status,  # License
                "1.0",  # License_Version
                foreign_id,  # Foreign Identifier
                width,  # Width of the image
                height,  # height of the image
                creator_name,  # Creator Name
                None,  # Creator URl
                title,  # Title of the Image
                meta_data,  # Meta Data
                None,  # Raw Tags
                'f',  # Watermarked
                None,  # Source
            )
    return images_till_now



def create_meta_data(data):
    meta_data = {}
    meta_data['accession_number'] = data.get('accession_number', '')
    meta_data['technique'] = data.get('technique', '')
    meta_data['date'] = data.get('date', '')
    meta_data['credit_line'] = data.get('creditline', '')
    meta_data['classificatoin'] = data.get('type', '')
    meta_data['culture'] = ','.join(list(filter(
        None, data.get('culture', ''))))
    meta_data['tombstone'] = data.get('tombstone', '')
    return meta_data


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Cleveland Museum of Art API',
        add_help=True
    )
    parser.add_argument(
        '--date',
        help='Identify the images uploaded on a date(YY-MM-DD)'
    )
    args = parser.parse_args()
    if args.date:
        date = args.date
    else:
        date_obj = datetime.now() - timedelta(days=2)
        date = datetime.strftime(date_obj, '%Y-%m-%d')

    main(date)

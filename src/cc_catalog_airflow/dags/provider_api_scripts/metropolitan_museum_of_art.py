import logging
import argparse
import os
from datetime import datetime, timedelta, timezone
import common.requester as requester
import common.storage.image as image_class

PROVIDER = 'Metropolitan Museum of Art'
DELAY = 1.0


CONTACT_EMAIL = os.getenv('WM_SCRIPT_CONTACT')
UA_STRING = (
    f'CC-Catalog/0.1 (https://creativecommons.org; {CONTACT_EMAIL})'
)
DEFAULT_REQUEST_HEADERS = {
    'User-Agent': UA_STRING
}

delayed_requester = requester.DelayedRequester(DELAY)
image_store = image_class.ImageStore(provider=PROVIDER)

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


def main(date=None):
    logger.info(f'Begin: Met Museum API requests')
    
    fetch_the_object_id = get_object_ids(date)
    if fetch_the_object_id:
        logger.info(f'Total object found {fetch_the_object_id[0]}')
        extract_the_data(fetch_the_object_id[1])
        
    total_images = image_store.commit()
    logger.info(f'Total CC0 images recieved {total_images}')



def get_object_ids(date):
    query_date = ''
    if date:
        query_date = '?metadataDate={}'.format(date)
    
    endpoint = 'https://collectionapi.metmuseum.org/public/collection/v1/objects{}'.format(query_date)
    response = _get_response_json(None, endpoint)
    
    if response:
        total_object_ids = response['total']
        object_ids = response['objectIDs']
    else:
        logger.warning(f'No content available')
        return None
    
    return [total_object_ids, object_ids]


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


def extract_the_data(object_ids):
    for i in object_ids:
        get_data_for_each_image(i)
    

def get_data_for_each_image(object_id):

    endpoint = 'https://collectionapi.metmuseum.org/public/collection/v1/objects/{}'.format(object_id)
    
    object_json = _get_response_json(None, endpoint)

    if object_json is None:
        logger.error('Unable to process object ID : {}'.format(object_id))
        return None

    message = object_json.get('message')
    if message:
        logger.warning(f'{message} : {object_id}')
        return None
    
    isCC0 = object_json.get('isPublicDomain')
    if (isCC0 is None) or isCC0 is False:
        logger.warning('CC0 license not detected')
        return None

    foreign_url = object_json.get('objectURL', None)
    if foreign_url is None:
        logger.warning(f'No landing page detected for: {object_id}')
        return None
    
    image_info = object_json.get('primaryImage', None)
    if image_info is None:
        logger.warning(f'No image found for {object_id}')
        return None

    title = object_json.get('title')
    creator_name = object_json.get('artistDisplayName')
    foreign_id = object_id
    meta_data = create_meta_data(object_json)
    image_url = image_info

    thumbnail = ''
    if '/origina/' in image_url:
        thumbnail = image_url.replace('/original/', '/web-large/')
    
    other_images = object_json.get('additionalImages', None)
    
    if other_images is not None and len(other_images)>1:
        extra_image_index = 1
        meta_data['set'] = foreign_url
    
    image_store.add_item(
        foreign_url,  # foreign url of image
        image_url,  # image url
        thumbnail,  # thubnail url
        None,  # license URl
        'cc0',  # license
        '1.0',  # license verion
        foreign_id,  # foreign identifier
        None,  # width
        None,  # height
        creator_name,  # creator name
        None,  # creator url
        title,  # title
        meta_data,  # meta data
        None,
        'f',
        None
    )

    for image in other_images:
        foreign_id = '{}-{}'.format(object_id, extra_image_index)
        image_url = image
        thumbnail = ''

        if image_url:
            if '/original/' in image_url:
                image_url.replace('/original/', '/web-image/')
        
        image_store.add_item(
            foreign_url,  # foreign url of image
            image_url,  # image url
            thumbnail,  # thubnail url
            None,  # license URl
            'cc0',  # license
            '1.0',  # license verion
            foreign_id,  # foreign identifier
            None,  # width
            None,  # height
            creator_name,  # creator name
            None,  # creator url
            title,  # title
            meta_data,  # meta data
            None,
            'f',
            None
    )

    

def create_meta_data(object_json):
    meta_data = {}

    meta_data['accession_number'] = object_json.get('accessionNumber', None)
    meta_data['classification'] = object_json.get('classification', None)
    meta_data['culture'] = object_json.get('culture', None)
    meta_data['date'] = object_json.get('objectDate', None)
    meta_data['medium'] = object_json.get('medium', None)
    meta_data['credit_line'] = object_json.get('creditLine', None)

    return meta_data


if __name__ == '__main__':
    mode = 'date :'
    parser = argparse.ArgumentParser(
        description='Metropolitan Museum of Art API',
        add_help=True
    )
    parser.add_argument(
        '--date',
        help='Fetches all the artwork uploaded after given date'
    )
    parser.add_argument(
        '--mode',
        choices=['default', 'all'],
        help='Identify all artworks from the previous day [default]'
         'or process the entire collection [all].'
    )
    args = parser.parse_args()
    if args.date:
        date = args.date
    
    elif args.mode:
        if str(args.mode) == 'default':
            date_obj = datetime.now() - timedelta(days=1)
            date = datetime.strftime(date_obj, '%Y-%m-%d')
        else:
            date = None
            mode = 'All CC0 Artworks'
    else:
        date_obj = datetime.now() - timedelta(days=1)
        date = datetime.strftime(date_obj, '%Y-%m-%d')

    mode += date if date is not None else ''
    logger.info(f'Processing for {mode}')
    
    main(date)

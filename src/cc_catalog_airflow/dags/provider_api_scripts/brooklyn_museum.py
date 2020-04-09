"""
Content Provider:       Brooklyn Museum

ETL Process:            Use the API to identify all CC licensed artworks.

Output:                 TSV file containing the images and respective meta-data.

Notes:                  https://www.brooklynmuseum.org/opencollection/api
                        3000 calls per day (per API Key)
"""
import logging
import os
import re
from common.requester import DelayedRequester
from common.storage import image

logger = logging.getLogger(__name__)

DELAY = 3.0  # time delay (in seconds)
RETRIES = 3
API_KEY = os.getenv('BROOKLYN_MUSEUM_API_KEY')
PROVIDER = 'brooklynmuseum'
LIMIT = 10
ENDPOINT = 'https://www.brooklynmuseum.org/api/v2/'

logging.basicConfig(format='%(asctime)s: [%(levelname)s - Brooklyn Museum API] =======> %(message)s', level=logging.INFO)

delayed_requester = DelayedRequester(DELAY)
image_store = image.ImageStore(provider=PROVIDER)

DEFAULT_QUERY_PARAM = {
    "has_images": 1,
    "rights_type_permissive": 1,
    "limit": LIMIT,
    "offset": 0
}


def main():
    logger.info('Begin: Brooklyn Museum API requests')
    condition = True
    offset = 0
    image_count = 0
    headers = {'api_key': API_KEY}

    while condition:
        query_param = _build_query_param(offset)
        endpoint = '{0}object?has_images=1&rights_type_permissive=1&limit={1}&offset={2}'.format(ENDPOINT, LIMIT, offset)
        response_json, total_images = _get_response(query_param, headers=headers, endpoint=endpoint)
        if response_json is not None and total_images != 0:
            objIDs = list(obj['id'] for obj in response_json['data'])
            for obj in objIDs:
                endpoint = '{0}object/{1}'.format(ENDPOINT, obj)
                response = delayed_requester.get(endpoint, params=None, headers=headers)
                result = _extract_response_json(response)
                image_count = _handle_response(result, obj)
            logger.info(f'Total images till now {image_count}')
            offset += LIMIT
        else:
            logger.error('No more images to process')
            logger.info('Exiting')
            condition = False
    image_count = image_store.commit()
    logger.info(f'Total number of images received {image_count}')


def _build_query_param(offset=0, default_query_param=DEFAULT_QUERY_PARAM):
    query_param = default_query_param.copy()
    query_param.update(
        offset=offset
    )
    return query_param


def _get_response(query_param, headers, endpoint=ENDPOINT, retries=RETRIES):
    response_json, total_images = None, 0
    for tries in range(retries):
        response = delayed_requester.get(
                    endpoint,
                    params=None,
                    headers=headers
                    )
        if response.status_code == 200 and response is not None:
            try:
                response_json = response.json()
                total_images = len(response_json['data'])
            except Exception as e:
                logger.warning(f'response not captured due to {e}')
                response_json = None
            if response_json is not None and total_images is not None:
                break

        logger.info('Retrying \n'
                    f'endpoint -- {endpoint} \t'
                    f' with parameters -- {query_param} ')
    if tries == retries-1 and ((response_json is None) or
                               (total_images is None)):
        logger.warning('No more tries remaining. Returning Nonetypes.')
        return None, 0
    else:
        return response_json, total_images


def _handle_response(object_data, _object_id):
    meta_data = {}
    img_url = ''
    width = ''
    height = ''
    foreign_id = ''
    foreign_url = ''
    title = ''
    creator = ''
    license = ''
    version = ''
    rights_info = object_data.get('rights_type')
    if rights_info is None or 'creative commons' not in rights_info.get('name').lower():
        logging.warning('License not detected!')
        return None

    license_url = _get_license_url(rights_info)
    if license_url is None:
        return None

    title = object_data.get('title', '')

    # the API doesnt provide a direct link to the landing page. Exception provided below
    foreign_url = 'https://www.brooklynmuseum.org/opencollection/objects/{}'.format(_object_id)
    artists = object_data.get('artists')
    artist_info = [{'name': artist['name'], 'nationality': artist['nationality']} for artist in artists]
    meta_data = _get_meta_data(object_data)
    if artist_info:
        creator = artist_info[0].get('name')
        meta_data['artist_info'] = artist_info

    image_info = object_data.get('images')
    if not image_info:
        logging.warning('Image not detected for object {}'.format(_object_id))
        return None

    if len(image_info) > 1:
        meta_data['set'] = foreign_url

    for img in image_info:
        foreign_id = img.get('id', '')
        img_url = img.get('largest_derivative_url', '')

        if not img_url:
            logging.warning('Image not detected for object {}'.format(_object_id))
            continue

        thumbnail = img.get('standard_size_url', '')
        lg_deriv = img.get('largest_derivative')
        if lg_deriv:
            # get the image dimensions
            derivatives = img.get('derivatives')
            if type(derivatives) is list:
                dimensions = [(dim.get('width'), dim.get('height')) for dim in derivatives if str(dim.get('size')) == str(lg_deriv)]
                width = dimensions[0][0]
                height = dimensions[0][1]

        meta_data['caption'] = img.get('caption')
        meta_data['credit'] = img.get('credit')

        total_images = image_store.add_item(
            foreign_landing_url=foreign_url,
            image_url=img_url,
            thumbnail_url=thumbnail,
            license_=license_url,
            license_version=version,
            width=width,
            height=height,
            creator=creator,
            title=title,
            meta_data=meta_data
            )

    return total_images


def _get_license_url(rights_info):
    if "creative commons" not in rights_info.get("name").lower():
        return None
    license_url = re.search('https://creativecommons.org/licenses/[^\s]+',
                            rights_info.get('description'))
    license_url = license_url.group(0).strip()
    return license_url


def _get_meta_data(object_data):
    meta_data['credit_line'] = object_data.get('credit_line')
    meta_data['medium'] = object_data.get('medium')
    meta_data['description'] = object_data.get('description')
    meta_data['date'] = object_data.get('object_date')
    meta_data['credit_line'] = object_data.get('period')
    meta_data['classification'] = object_data.get('classification')
    meta_data['accession_number'] = object_data.get('accession_number')
    return meta_data


def _extract_response_json(response):
    if response is not None and response.status_code == 200:
        try:
            response_json = response.json()
        except Exception as e:
            logger.warning(f'Could not get image_data json.\n{e}')
            response_json = None
    else:
        response_json = None

    return response_json


if __name__ == '__main__':
    main()

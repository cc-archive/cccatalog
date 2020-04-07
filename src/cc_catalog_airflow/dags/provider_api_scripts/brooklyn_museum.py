"""
Edit to run tests
Content Provider:       Brooklyn Museum

ETL Process:            Use the API to identify all CC licensed artworks.

Output:                 TSV file containing the images and respective meta-data.

Notes:                  https://www.brooklynmuseum.org/opencollection/api
                        3000 calls per day (per API Key)
"""
import logging
import time
import os
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


def _handle_response(objectData, _objectID):
    metaData = {}
    imgURL = ''
    width = ''
    height = ''
    foreignID = ''
    foreignURL = ''
    title = ''
    creator = ''
    license = ''
    version = ''
    rightsInfo = objectData.get('rights_type')
    if rightsInfo is None or 'creative commons' not in rightsInfo.get('name').lower():
        logging.warning('License not detected!')
        return None

    licenseURL = _get_license_url(rightsInfo)
    if licenseURL is None:
        return None

    title = objectData.get('title', '')

    # the API doesnt provide a direct link to the landing page. Exception provided below
    foreignURL = 'https://www.brooklynmuseum.org/opencollection/objects/{}'.format(_objectID)
    artists = objectData.get('artists')
    artistInfo = [{'name': artist['name'], 'nationality': artist['nationality']} for artist in artists]
    metaData = _get_metadata(objectData)
    if artistInfo:
        creator = artistInfo[0].get('name')
        metaData['artist_info'] = artistInfo

    
    imageInfo = objectData.get('images')
    if not imageInfo:
        logging.warning('Image not detected for object {}'.format(_objectID))
        return None

    if len(imageInfo) > 1:
        metaData['set'] = foreignURL

    for img in imageInfo:
        foreignID = img.get('id', '')
        imgURL = img.get('largest_derivative_url', '')

        if not imgURL:
            logging.warning('Image not detected for object {}'.format(_objectID))
            continue

        thumbnail = img.get('standard_size_url', '')
        lgDeriv = img.get('largest_derivative')
        if lgDeriv:
            # get the image dimensions
            derivatives = img.get('derivatives')
            if type(derivatives) is list:
                dimensions = [(dim.get('width'), dim.get('height')) for dim in derivatives if str(dim.get('size')) == str(lgDeriv)]
                width = dimensions[0][0]
                height = dimensions[0][1]

        metaData['caption'] = img.get('caption')
        metaData['credit'] = img.get('credit')

        total_images = image_store.add_item(
            foreign_landing_url=foreignURL,
            image_url=imgURL,
            thumbnail_url=thumbnail,
            license_=licenseURL,
            license_version=version,
            width=width,
            height=height,
            creator=creator,
            title=title,
            meta_data=metaData
            )

    return total_images


def _get_license_url(rightsInfo):
    if "creative commons" not in rightsInfo.get("name").lower():
        return None
    licenseURL = re.search('https://creativecommons.org/licenses/[^\s]+',
                            rightsInfo.get('description'))
    licenseURL = licenseURL.group(0).strip()
    return licenseURL


def _get_metadata(objectData):
    metaData['credit_line'] = objectData.get('credit_line')
    metaData['medium'] = objectData.get('medium')
    metaData['description'] = objectData.get('description')
    metaData['date'] = objectData.get('object_date')
    metaData['credit_line'] = objectData.get('period')
    metaData['classification'] = objectData.get('classification')
    metaData['accession_number'] = objectData.get('accession_number')
    return metaData


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

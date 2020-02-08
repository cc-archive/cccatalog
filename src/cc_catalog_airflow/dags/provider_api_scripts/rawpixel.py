from common.storage.image import ImageStore
from common.requester import DelayedRequester
import requests
import time
import logging
import re
import json
from urllib.parse import urlparse, parse_qs

DELAY   = 1.0 #time delay (in seconds)
PROVIDER = 'rawpixel'
FILE    = 'rawpixel_{}.tsv'.format(int(time.time()))

logging.basicConfig(format='%(asctime)s: [%(levelname)s - RawPixel API] =======> %(message)s', level=logging.INFO)

delayed_requester = DelayedRequester(DELAY)
image_store = ImageStore(provider=PROVIDER,output_file=FILE)

def sanitizeString(_data):
    if _data is None:
        return ''
    else:
        _data = str(_data)

    _data       = _data.strip()
    _data       = _data.replace('"', "'")
    _data       = re.sub(r'\n|\r', ' ', _data)
    #_data      = re.escape(_data)

    backspaces  = re.compile('\b+')
    _data       = backspaces.sub('', _data)
    _data       = _data.replace('\\', '\\\\')

    return re.sub(r'\s+', ' ', _data)

def requestContent(_url, _query_params=None, _headers=None):
    logging.info('Processing request: {}'.format(_url))

    response = delayed_requester.get(
        _url,
        params=_query_params,
        headers=_headers
    )

    try:
        if response.status_code == requests.codes.ok:
            return response.json()
        else:
            logging.warning('Unable to request URL: {}. Status code: {}'.format(url, response.status_code))
            return None

    except Exception as e:
        logging.error('There was an error with the request.')
        logging.info('{}: {}'.format(type(e).__name__, e))
        return None

def getImageList(_page=1):
    endpoint    = 'https://api.rawpixel.com/api/v1/search'
    query_params =  {
        'freecc0': 1,
        'html': 0,
        'page': _page
    }
    request = requestContent(endpoint, _query_params=query_params)

    if request.get('results'):
        return [request.get('total'), request.get('results')]

    else:
        return [None, None]

def _process_image_data(_image):
    startTime   = time.time()

    #verify the license and extract the metadata
    foreignID   = ''
    foreignURL  = ''
    imgURL      = ''
    width       = ''
    height      = ''
    thumbnail   = ''
    tags        = ''
    title       = ''
    owner       = ''
    license     = 'cc0'
    version     = '1.0'
    tags        = {}

    if _image.get('freecc0'):
        #get the image identifier
        foreignID = _image.get('id', '')

        #get the landing page
        foreignURL = _image.get('url')

        if not foreignURL:
            logging.warning('Landing page not detected for image ID: {}'.format(foreignID))
            return None

        imgURL = _image.get('image_opengraph')
        if imgURL:
            #extract the dimensions from the query params because the dimensions in the metadata are at times inconsistent with the rescaled images
            queryParams = urlparse(imgURL)
            width       = parse_qs(queryParams.query).get('w', [])[0] #width
            height      = parse_qs(queryParams.query).get('h', [])[0] #height

            thumbnail   = _image.get('image_400', '')
        else:
            logging.warning('Image not detected in URL: {}'.format(foreignURL))
            return None

        title = sanitizeString(_image.get('image_title', ''))

        owner = sanitizeString(_image.get('artists', ''))
        owner = owner.replace('(Source)', '').strip()

        keywords        = _image.get('keywords_raw')
        if keywords:
            keywordList = keywords.split(',')
            keywordList = list(filter(lambda word: word.strip() not in ['cc0', 'creative commons', 'creative commons 0'], keywordList))

            tags        = [{'name': sanitizeString(tag), 'provider': 'rawpixel'} for tag in keywordList]

    # TODO: How to get license_url, creator, creator_url, source?
    return image_store.add_item(
        foreign_landing_url=foreignURL,
        image_url=imgURL,
        license_=license,
        license_version=str(version),
        foreign_identifier=str(foreignID),
        width=str(width) if width else None,
        height=str(height) if height else None,
        title=title if title else None,
        raw_tags=json.dumps(tags, ensure_ascii=False) if bool(tags) else None,
    )

def main():
    page    = 1
    imgCtr  = 0
    isValid = True

    logging.info('Begin: RawPixel API requests')

    total, result = getImageList(page)

    while (imgCtr < total) and isValid:
        logging.info('Processing page: {}'.format(page))

        startTime = time.time()
        for img in result:
            total_images = _process_image_data(img)
            imgCtr = total_images if total_images else imgCtr

        total_images = image_store.commit()

        page += 1
        total, result = getImageList(page)

        if not result:
            isValid = False

        if not total:
            total = 0
            isValid = False

    logging.info('Total images: {}'.format(imgCtr))
    logging.info('Terminated!')


if __name__ == '__main__':
    main()

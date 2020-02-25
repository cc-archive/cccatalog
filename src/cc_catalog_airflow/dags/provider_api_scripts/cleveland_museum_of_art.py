import logging
import common.requester as requester
import common.storage.image as image_class
LIMIT = 500   # It sets the limit to how many images we want to pull at a time
DELAY = 5.0   # Time Delay between consecutive API requests(in seconds)
PROVIDER = 'clevelandmuseum'
ENDPOINT = 'https://openaccess-api.clevelandart.org/api/artworks'

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.INFO
)

logger = logging.getLogger(__name__)


DEFAULT_QUERY_PARAMS = {
    'cc0': '1',
    'has_image': '1',
    'limit': LIMIT,
    'skip': '0'
}

delayed_requester = requester.DelayedRequester(DELAY)
image_store = image_class.ImageStore(provider=PROVIDER)


def main():
    logger.info(f'Starting Cleveland API requets')
    condition = True
    offset = 0
    while condition:
        query_params = _build_query_params(offset)
        response = _get_response_json(query_params)
        if response is not None and response.get('data', []):
            batch = response['data']
            images_till_now = _handle_the_response(batch)
            logger.info(f'Total Images till now {images_till_now}')
            offset = offset + LIMIT
        else:
            logger.info(f'No more images to process so exiting the loop')
            condition = False
    total_images = image_store.commit()
    logger.info(f'Total number of images received {total_images}')



def _build_query_params(
    offset
):
    query_params = DEFAULT_QUERY_PARAMS.copy()
    query_params.update(
        skip=offset
    )
    return query_params


def _get_response_json(
        query_params,
        endpoint=ENDPOINT,
        retries=5,
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

    if (
            response_json is None or response_json.get('error') is not None
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




def _handle_the_response(response):
    for data in response:
        foreign_url = data.get('url', None)
        single_image = data.get('images')
        images_till_now = 0
        if single_image is None:
            continue

        image_url, key = _choose_image_version(single_image)
        if image_url is None or key is None:
            continue
        width = single_image[key]['width']

        height = single_image[key]['height']

        foreign_id = data.get('id', image_url)

        license_status = data.get('share_license_status', None).lower()

        title = data.get('title', '')

        creator_info = data.get('creators', {})
        creator_name = None
        if creator_info:
            creator_name = creator_info[0].get('description', '')
        meta_data = _create_meta_data(data)
        images_till_now = image_store.add_item(
            foreign_landing_url=foreign_url,  # Foreign Landing URl
            image_url=image_url,  # Image URL
            license_=license_status,  # License
            license_version="1.0",  # License_Version
            foreign_identifier=foreign_id,  # Foreign Identifier
            width=width,  # Width of the image
            height=height,  # height of the image
            creator=creator_name,  # Creator Name
            title=title,  # Title of the Image
            meta_data=meta_data,  # Meta Data
        )
    return images_till_now


def _choose_image_version(single_image):
    if single_image.get('web'):
        key = 'web'
        image_url = single_image.get('web').get('url')
    elif single_image.get('print'):
        key = 'print'
        image_url = single_image.get('print').get('url')
    elif single_image.get('full'):
        key = 'full'
        image_url = single_image.get('full').get('url')
    else:
        key = None
        image_url = None
    return image_url, key


def _create_meta_data(data):
    meta_data = {}
    meta_data['accession_number'] = data.get('accession_number', '')
    meta_data['technique'] = data.get('technique', '')
    meta_data['date'] = data.get('date', '')
    meta_data['credit_line'] = data.get('creditline', '')
    meta_data['classification'] = data.get('type', '')
    meta_data['culture'] = ','.join(
        [i for i in data.get('culture', []) if i is not None]
    )

    meta_data['tombstone'] = data.get('tombstone', '')
    return meta_data


if __name__ == '__main__':
    main()

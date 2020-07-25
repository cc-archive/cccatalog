import os
import logging
from urllib.parse import urlparse
from common.requester import DelayedRequester
from common.storage.image import ImageStore

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

LIMIT = 100
DELAY = 1.0
RETRIES = 3
PROVIDER = "iconfinder"
SEARCH_ENDPOINT = "https://api.iconfinder.com/v4/icons/search"
ITEM_ENDPOINT = "https://api.iconfinder.com/v4/icons/"
API_KEY = os.getenv("ICONFINDER_API_KEY")

delay_request = DelayedRequester(delay=DELAY)
image_store = ImageStore(provider=PROVIDER)

DEFAULT_QUERY_PARAM = {
    "query": ":",
    "count": LIMIT,
    "offset": 0,
    "premium": 0
}

HEADERS = {
    'authorization': f'Bearer {API_KEY}'
}


def main():
    offset = 0
    condition = True
    while condition:
        query_param = _get_query_param(
            offset=offset
        )
        response_json = _request_handler(
            params=query_param
        )
        icon_batch = response_json.get("icons")
        if type(icon_batch) == list and len(icon_batch) > 0:
            _process_icon_batch(icon_batch)
            logger.info(
                f"Images collected in this batch: {image_store.total_images}"
            )
            offset += LIMIT
        else:
            condition = False
    image_store.commit()
    logger.info(f"total images : f{image_store.total_images}")


def _get_query_param(
        offset=0,
        default_query_param=DEFAULT_QUERY_PARAM,
        ):
    query_param = default_query_param.copy()
    query_param["offset"] = offset
    return query_param


def _request_handler(
        endpoint=SEARCH_ENDPOINT,
        params=None,
        headers=HEADERS,
        retries=RETRIES
        ):
    for retry in range(retries):
        response = delay_request.get(
            endpoint,
            params=params,
            headers=headers
        )
        if response.status_code == 200:
            try:
                response_json = response.json()
                break
            except Exception as e:
                logger.error(f"Request failed due to {e}")
                response_json = None
        else:
            response_json = None

    return response_json


def _process_icon_batch(icon_batch):
    for icon in icon_batch:

        icon_id = icon.get("icon_id")
        if icon_id is None:
            continue

        icon_data = _request_handler(
            endpoint=ITEM_ENDPOINT+str(icon_id)
        )
        if icon_data is None:
            continue

        iconset = icon_data.get("iconset")
        if iconset is None:
            continue
        license_url = _get_license(
            iconset.get("license")
        )
        if license_url is None:
            continue
        foreign_landing_url = (
            "https://www.iconfinder.com/icons/" + str(icon_id)
        )
        image_url, thumbnail_url, height, width = _get_images(
            icon_data.get("raster_sizes")
        )
        if image_url is None:
            continue

        creator = _get_creator(
            iconset.get("author")
        )
        meta_data = _get_metadata(icon_data)
        image_store.add_item(
            foreign_identifier=icon_id,
            foreign_landing_url=foreign_landing_url,
            image_url=image_url,
            license_url=license_url,
            height=height,
            width=width,
            thumbnail_url=thumbnail_url,
            creator=creator,
            meta_data=meta_data
        )


def _get_license(license_):
    license_url = None
    if type(license_) == dict:
        url = license_.get("url")
        if urlparse(url).netloc == "creativecommons.org":
            license_url = url
    return license_url


def _get_images(raster_sizes):
    image_url, thumbnail_url = None, None
    height, width = None, None
    if type(raster_sizes) == list:
        for raster in raster_sizes:
            if raster.get("size") == 512:
                raster_format = raster.get("formats")
                if type(raster_format) == list:
                    image_url = thumbnail_url = (
                        raster_format[0].get("preview_url")
                    )
                    height = 512
                    width = 512
    return image_url, thumbnail_url, height, width


def _get_creator(author):
    creator = None
    if type(author) == dict:
        creator = author.get("name")

    if creator is None:
        logger.warning("No creator found")
    return creator


def _get_metadata(icon_data):
    meta_data = {}
    categories = icon_data.get("categories")
    if type(categories) == list:
        categories_list = [
            ct.get("name", "")
            for ct in categories
        ]
        meta_data["categories"] = ','.join(categories_list)

    styles = icon_data.get("styles")
    if type(styles) == list:
        styles_list = [
            st.get("name", "")
            for st in styles
        ]
        meta_data["styles"] = ','.join(styles_list)

    meta_data["published_date"] = icon_data.get("published_at")

    return meta_data


if __name__ == "__main__":
    main()

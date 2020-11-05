"""
Content Provider:       Scan The World
ETL Process:            Use the API to identify all CC licensed images.
Output:                 TSV file containing the images and the
                        respective meta-data.
Notes:                  https://www.myminifactory.com/api/v2/
                        Rate limit: 5 requests per second
"""

import os
import logging
from common.requester import DelayedRequester
from common.storage.image import ImageStore
from util.loader import provider_details as prov

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s:  %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

DELAY = 1
LIMIT = 100
RETRIES = 3
MAX_DESCRIPTION_LENGTH = 2000
PROVIDER = prov.STW_DEFAULT_PROVIDER
REQUEST_TYPE = "search"
ENDPOINT = f"https://www.myminifactory.com/api/v2/{REQUEST_TYPE}"
API_KEY = os.getenv("SCAN_THE_WORLD_KEY")

delayed_requester = DelayedRequester(delay=DELAY)
image_store = ImageStore(provider=PROVIDER)

HEADERS = {"key": API_KEY}

DEFAULT_QUERY_PARAM = {"cat": 112, "rec_cat": 1, "page": 1, "per_page": LIMIT}

LICENSES = {
    "Creative Commons - Public Domain": ("cc0", "1.0"),
    "Creative Commons - Attribution - Noncommercial - ShareAlike":
    ("by-nc-sa", "2.0"),
}


def main():
    logger.info("Begin: Stw provider script")
    condition = True
    page = 1
    while condition:
        query_param = _get_query_param(page=page)
        object_list = _get_object_list(query_param=query_param)
        if type(object_list) == list and len(object_list) > 0:
            _ = _process_object_list(object_list)
            logger.debug(f"Images till now {image_store.total_images}")
            page = page + 1
        else:
            condition = False
    image_store.commit()
    logger.info(f"Total images recieved {image_store.total_images}")


def _get_query_param(page, default_query_param=DEFAULT_QUERY_PARAM):
    query_param = default_query_param.copy()
    query_param.update(page=page)
    return query_param


def _get_object_list(query_param, headers=HEADERS,
                     endpoint=ENDPOINT, retries=RETRIES):
    for r in range(retries):
        response = delayed_requester.get(
            endpoint,
            headers=headers,
            params=query_param,
        )
        logger.debug("response.status_code: {response.status_code}")
        if response is not None and response.status_code == 200:
            break
    if (r == retries - 1) and (
     response is None or response.status_code != 200):
        logger.warning("No more tries remaining. Returning Nonetypes.")
        return None
    response_json = _extract_response_json(response)
    object_list = _extract_object_list_from_json(response_json)

    return object_list


def _extract_response_json(response):
    if response is not None and response.status_code == 200:
        try:
            response_json = response.json()
        except Exception as e:
            logger.warning(f"Could not get image_data json.\n{e}")
            response_json = None
    else:
        response_json = None

    return response_json


def _extract_object_list_from_json(response_json):
    if response_json is None:
        object_list = None
    else:
        object_list = response_json.get("items")

    return object_list


def _process_object_list(object_list):
    for obj in object_list:
        total_images = _process_object(obj)
    return total_images


def _process_object(obj):
    license = obj.get("license")
    if license not in LICENSES.keys():
        return None
    license_, license_version = LICENSES.get(license)
    foreign_landing_url = obj.get("url")
    title = obj.get("name")
    raw_tags = obj.get("tags")
    creator = obj.get("designer")
    creator_url = creator.get("profile_url")
    meta_data = _create_meta_data_dict(obj)
    image_list = obj.get("images")
    for img in image_list:
        foreign_id = img.get("id")
        image_original = img.get("original")
        image_url = image_original.get("url")
        image_thumbnail = img.get("thumbnail")
        thumbnail_url = image_thumbnail.get("url")

        total_images = image_store.add_item(
            foreign_landing_url=foreign_landing_url,
            image_url=image_url,
            thumbnail_url=thumbnail_url,
            license_=license_,
            license_version=license_version,
            foreign_identifier=foreign_id,
            creator_url=creator_url,
            title=title,
            meta_data=meta_data,
            raw_tags=raw_tags,
        )

    return total_images


def _create_meta_data_dict(obj, max_description_length=MAX_DESCRIPTION_LENGTH):
    image_meta_data = {}
    description_text = obj.get("description")
    if description_text is not None:
        image_meta_data["description"] = " ".join(
            description_text.split(" ")[:MAX_DESCRIPTION_LENGTH]
        )
    image_meta_data["views"] = obj.get("views")
    image_meta_data["likes"] = obj.get("likes")
    image_meta_data["pub_date"] = obj.get("published_at")

    return {k: v for k, v in image_meta_data.items() if v is not None}


if __name__ == "__main__":
    main()

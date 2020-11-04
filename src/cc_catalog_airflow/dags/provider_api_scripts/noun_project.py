"""
Content Provider:       The Noun Project Icon API

ETL Process:            Use the API to identify all CC licensed images.

Output:                 TSV file containing the images and the
                        respective meta-data.

Notes:                  http://api.thenounproject.com/
                        Rate limit: 5000 requests per month
"""

import os
import logging
import common.requester as requester
import common.storage.image as image
from requests_oauthlib import OAuth1
from util.loader import provider_details as prov

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s:  %(message)s',
    level=logging.INFO
)

logger = logging.getLogger(__name__)

DELAY = 5
PROVIDER = prov.NOUN_PROJECT_DEFAULT_PROVIDER
ENDPOINT = 'http://api.thenounproject.com/'
API_KEY = os.getenv('THE_NOUN_PROJECT_API_KEY')
SECRET_KEY = os.getenv('THE_NOUN_PROJECT_SECRET_KEY')
AUTH = OAuth1(API_KEY, SECRET_KEY)
SITE = 'https://thenounproject.com/'
delayed_requester = requester.DelayedRequester(DELAY)
image_store = image.ImageStore(provider=PROVIDER)


def main():
    logger.info("Begin: The Noun Project Icons API.")

    collections_list = _get_collections_list()
    for collection in collections_list:
        icons_list = _get_icons_list_from_collection(collection)
        _ = _process_icons_list(icons_list)

    total_images = image_store.commit()
    logger.info(f"Total Images: {total_images}")
    logger.info("Terminating Script!")


def _get_collections_list(
    auth=AUTH,
    endpoint=ENDPOINT,
    retries=3,
    total_pages=2366  # Change this when total pages for collections changes
):
    collections_list = []
    page = 1
    while (page <= total_pages):
        query_params = _build_query_param(page=page)
        page += 1
        json_response = delayed_requester.get_response_json(
            endpoint=(endpoint + "collections"),
            retries=retries,
            query_params=query_params,
        )

        _collections_list = _extract_collections_list_from_json(json_response)
        if _collections_list is None:
            break
        for collection in _collections_list:
            collections_list.append(collection)

    return collections_list


def _build_query_param(page=1):
    query_params = {}
    query_params.update({"page": page})

    return query_params


def _extract_collections_list_from_json(
        json_response
):
    if (
        json_response is None
        or json_response.get("collections") is None
        or len(json_response.get("collections")) == 0
    ):
        collections_list = None
    else:
        collections_list = []
        _collections_list = json_response.get("collections")
        for collection in _collections_list:
            collections_list.append(collection.get("slug"))

    return collections_list


def _get_icons_list_from_collection(
    collection=None,
    retries=3,
    endpoint=ENDPOINT,
    auth=AUTH
):
    icon_count = get_icon_count(collection=collection)
    total_pages = _find_total_pages_for_collection(icon_count)
    icons_list = []
    page = 1
    while (page <= total_pages):
        query_params = _build_query_param(page=page)
        page += 1
        json_response = delayed_requester.get_response_json(
            # Request is of the form:
            # "http://api.thenounproject.com/collection/{coll_name}/
            # icons?page={page_no}"
            endpoint=(endpoint + "collection/" + collection + "icons"),
            retries=retries,
            query_params=query_params,
        )

        _icons_list = _extract_icons_list_from_json(json_response)
        if _icons_list is None:
            break
        for icon in _icons_list:
            icon.update({"collection": collection})
            icons_list.append(icon)

    if len(icons_list) == 0:
        logger.warning("No more tries remaining. Returning Nonetypes.")
        return None
    else:
        return icons_list


def _find_total_pages_for_collection(icon_count=None):
    pages = 0
    if icon_count is None or icon_count == 0:
        return pages

    if icon_count != 0:
        if icon_count % 50 == 0:
            return int(icon_count/50)
        else:
            return int(icon_count//50 + 1)
    return pages


def get_icon_count(
    endpoint=ENDPOINT,
    collection=None
):
    icon_count = None
    if collection is None:
        return icon_count

    json_response = delayed_requester.get_response_json(
        endpoint=(endpoint + "collection/" + collection),
        retries=3
    )

    if json_response:
        icon_count = json_response.get('collection', {}).get('icon_count')
        return icon_count
    else:
        return icon_count


def _extract_icons_list_from_json(json_response):
    if (
        json_response is None
        or json_response.get("icons") is None
        or len(json_response.get("icons")) == 0
    ):
        icons_list = None
    else:
        icons_list = json_response.get("icons")

    return icons_list


def _process_icons_list(icons_list):
    total_icons = 0
    if icons_list is not None:
        for icon in icons_list:
            total_icons = _process_icon(icon)

    return total_icons


def _process_icon(icon):
    logger.debug(f'Processing Icon: {icon}')

    foreign_landing_url = _get_foreign_landing_url(icon)
    image_url = icon.get("icon_url")
    license_url = _get_license_url(icon)
    thumbnail_url = icon.get("preview_url_84")
    foreign_identifier = icon.get("id")
    creator = icon.get("uploader", {}).get("name")
    creator_url = _get_creator_url(icon)
    title = icon.get("term_slug")
    raw_tags = icon.get("tags")
    watermarked = False

    return image_store.add_item(
        foreign_landing_url=foreign_landing_url,
        image_url=image_url,
        license_url=license_url,
        thumbnail_url=thumbnail_url,
        foreign_identifier=foreign_identifier,
        creator=creator,
        creator_url=creator_url,
        title=title,
        raw_tags=raw_tags,
        watermarked=watermarked
    )


def _get_foreign_landing_url(icon):
    foreign_landing_url = None
    site = SITE
    uploader_name = icon.get("uploader", {}).get("username")
    collection = icon.get("collection")
    id = icon.get("id")
    if uploader_name is None or id is None:
        return foreign_landing_url
    # foreign_landing_url is of the form:
    # https://thenounproject.com/uploader_name/collection/coll_name/?i=id
    foreign_landing_url = (
        site + uploader_name + '/collection/' + collection + '/?i=' + id
    )

    return foreign_landing_url


def _get_license_url(icon):
    license_url = None
    license_descr = icon.get("license_description")

    if license_descr is None:
        return license_url
    if license_descr == "public-domain":
        license_url = "https://creativecommons.org/publicdomain/zero/1.0/"
    elif license_descr == "creative-commons-attribution":
        license_url = (
                "https://creativecommons.org/licenses/by/3.0/us/legalcode"
            )

    return license_url


def _get_creator_url(icon):
    creator_url = None
    site = SITE
    creator = icon.get("uploader", {}).get("username")
    if creator is None:
        return creator_url
    creator_url = (site + creator + "/")

    return creator_url


if __name__ == "__main__":
    main()

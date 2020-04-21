from util.loader.cleanup import CleanupFunctions
from psycopg2.extras import Json


def test_tag_blacklist():
    tags = [
        {
            'name': 'cc0'
        },
        {
            'name': ' cc0'
        },
        {
            'name': 'valid',
            'accuracy': 0.99
        },
        {
            'name': 'valid_no_accuracy'
        },
        {
            'name': 'garbage:=metacrap',
        }
    ]
    result = str(CleanupFunctions.cleanup_tags(tags))
    expected = str(Json([
        {'name': 'valid', 'accuracy': 0.99},
        {'name': 'valid_no_accuracy'}
    ]
    ))

    assert result == expected


def tag_no_update():
    tags = [
        {
            'name': 'valid',
            'accuracy': 0.92
        }
    ]
    result = CleanupFunctions.cleanup_tags(tags)
    assert result is None


def test_accuracy_filter():
    tags = [
        {
            'name': 'inaccurate',
            'accuracy': 0.5
        },
        {
            'name': 'accurate',
            'accuracy': 0.999
        }
    ]
    result = str(CleanupFunctions.cleanup_tags(tags))
    expected = str(Json([
        {'name': 'accurate', 'accuracy': 0.999}
    ]))
    assert result == expected


def test_protocol_fix():
    bad_url = 'flickr.com'
    tls_support_cache = {}
    result = CleanupFunctions.cleanup_url(bad_url, tls_support_cache)
    expected = "'https://flickr.com'"

    bad_http = 'neverssl.com'
    result_http = CleanupFunctions.cleanup_url(bad_http, tls_support_cache)
    expected_http = "'http://neverssl.com'"
    assert result == expected
    assert result_http == expected_http

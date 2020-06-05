import csv
import statistics
import json
import os
import logging as log
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
from numbers import Real
from textwrap import dedent

# Fields indicating popularity expected in the meta_data column
popularity_fields = [
    'global_usage_count',
    'views'
]
# The percentile used to compute the normalizing constant
PERCENTILE = 0.85

DB_CONN_ID = os.getenv('OPENLEDGER_CONN_ID', 'postgres_openledger_testing')
PERCENTILE_FILE_CACHE = os.path.join(
    os.getenv('OUTPUT_DIR'), 'percentiles.json'
)
POPULARITY_DUMP_DEST = os.path.join(
    os.getenv('OUTPUT_DIR'), 'popularity_dump.tsv'
)
NORMALIZED_POPULARITY_DEST = os.path.join(
    os.getenv('OUTPUT_DIR'), 'normalized_popularity_dump.tsv'
)


# Cache provider constant so it doesn't need to be recomputed for every row
constants = {}


def _compute_percentiles_file(postgres_conn_id):
    _json = {}
    # Don't recompute the cache for 60 days
    cached_date = datetime.utcnow()
    expire_time = timedelta(days=60)
    cache_expires_date = cached_date + expire_time
    _json['expires'] = cache_expires_date.isoformat()

    percentiles = select_percentiles(
        postgres_conn_id, popularity_fields, PERCENTILE
    )
    _json['percentiles'] = percentiles
    log.info(f'New percentiles file: {_json}')
    with open(PERCENTILE_FILE_CACHE, 'w+') as percentile_f:
        percentile_f.write(json.dumps(_json))


def _get_percentiles(postgres, attempts=0):
    """ Return the PERCENTILEth value for each metric in a dictionary. """
    if attempts > 1:
        raise SystemError('Failed to generate percentiles file.')
    try:
        with open(PERCENTILE_FILE_CACHE, 'r') as percentile_f:
            percentiles = json.load(percentile_f)
            for popfield in popularity_fields:
                if popfield not in percentiles['percentiles']:
                    raise KeyError(
                        f'Expected field missing from popularity percentiles '
                        f'file cache: {popfield}. Actual: {percentiles}'
                    )
            now = datetime.utcnow()
            if datetime.fromisoformat(percentiles['expires']) < now:
                msg = 'Percentile cache expired and needs to be recomputed'
                log.info(msg)
                raise ValueError(msg)
            return percentiles['percentiles']
    except (FileNotFoundError, KeyError, ValueError) as e:
        # If the cache has expired or a field is missing, the cache will be
        # refreshed.
        log.info(f'Recomputing percentiles file because: {e}')
        _compute_percentiles_file(postgres)
        attempts += 1
        return _get_percentiles(postgres, attempts)


def _cache_constant(provider, metric, constant):
    constants[f'{provider}_{metric}'] = constant


def _get_constant(provider, metric, percentile, value):
    """ Get the constant from the cache or compute it. """
    try:
        constant = constants[f'{provider}_{metric}']
    except KeyError:
        constant = compute_constant(percentile, value)
        _cache_constant(provider, metric, constant)
    return constant


def compute_constant(percentile: float, percentile_value: Real):
    """
    Compute normalizing constant for each popularity source.
    :param percentile: The target percentile for each source (ex: 0.85)
    :param percentile_value: The value of the percentile (e.g.for
    `percenile=0.85`, this should be the 85th percentile of the metric)
    :return: A float representing the constant 'C' in the popularity formula.
    """
    return ((1 - percentile) / percentile) * percentile_value


def compute_popularity(metric_value: Real, source_constant: Real):
    return metric_value / (metric_value + source_constant)


def _generate_popularity_tsv(input_tsv, output_tsv, percentiles):
    """
    :param input_tsv: A file containing raw popularity data.
    :param output_tsv: A file that will serve as the destination for
    normalized popularity data.
    """
    popularity_tsv = csv.DictReader(input_tsv, delimiter='\t')
    fieldnames = ['identifier', 'normalized_popularity']
    output_tsv = csv.DictWriter(
        output_tsv, delimiter='\t', fieldnames=fieldnames
    )
    output_tsv.writeheader()
    for idx, row in enumerate(popularity_tsv):
        if idx == 0:
            continue
        normalized_scores = []
        for metric in popularity_fields:
            percentile_value = percentiles[metric]
            constant = _get_constant(
                row['provider'], metric, PERCENTILE, percentile_value
            )
            if metric in row:
                if not row[metric]:
                    continue
                float_metric = float(row[metric])
                normalized = compute_popularity(float_metric, constant) * 100
                normalized_scores.append(normalized)
        popularity = statistics.mean(normalized_scores)
        output_row = {
            'identifier': row['identifier'],
            'normalized_popularity': popularity
        }
        output_tsv.writerow(output_row)


def _build_popularity_dump_query(popularity_fields):
    """
    Given a list of fields used in popularity data calculations, build a query
    returning all rows with at least one popularity metric.
    """
    # SELECT predicate for each popularity field
    selection_qs = []
    # WHERE predicate excluding null values for each field
    field_not_null_qs = []
    for field in popularity_fields:
        selection_qs.append(f"meta_data->>'{field}' AS {field}")
        field_not_null_qs.append(f"meta_data->>'{field}' IS NOT NULL")
    selections = ', '.join(selection_qs)
    field_not_null = ' OR '.join(field_not_null_qs)
    return f"SELECT identifier, provider, {selections} FROM image " \
           f"WHERE ({field_not_null})"


def select_percentiles(postgres_conn_id, popularity_fields, percentile):
    """
    Given a list of fields that occur in the `meta_data` column, return the
    percentile for each field.
    """
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    field_queries = []
    for field in popularity_fields:
        field_queries.append(
            f"percentile_disc({percentile}) WITHIN GROUP "
            f"(ORDER BY meta_data->>'{field}') AS {field}"
        )
    select_predicate = ', '.join(field_queries)
    select = f'SELECT {select_predicate} from image'
    res = postgres.get_records(select)
    field_percentiles = {
        field: int(value) for field, value in zip(popularity_fields, res[0])
    }
    return field_percentiles


def dump_selection_to_tsv(postgres_conn_id, query, tsv_file_name):
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    query = dedent(
        f"COPY ({query}) TO STDOUT "
        f"WITH CSV HEADER DELIMITER E'\t'"
    )
    postgres.copy_expert(query, tsv_file_name)


def upload_normalized_popularity(postgres_conn_id, in_tsv):
    """ Write the `normalized_score` field from `in_tsv` to the catalog. """
    postgres = PostgresHook(postgres_conn_id=postgres_conn_id)
    postgres.copy_expert(
        "CREATE TEMP TABLE temp_popularity "
        "(identifier uuid, normalized_popularity text); "
        "COPY temp_popularity FROM STDIN WITH CSV HEADER DELIMITER E'\t'; "
        "UPDATE image SET meta_data = jsonb_set("
        "  meta_data, "
        "  '{normalized_popularity}',"
        "  temp_popularity.normalized_popularity::jsonb"
        ") FROM temp_popularity"
        "  WHERE image.identifier = temp_popularity.identifier;",
        in_tsv
    )

def main():
    log.info('Starting popularity job. . .')
    percentiles = _get_percentiles(DB_CONN_ID)
    dump_query = _build_popularity_dump_query(popularity_fields)
    log.info('Creating TSV of popularity data. . .')
    with open(POPULARITY_DUMP_DEST, 'w+') as in_tsv, \
            open(NORMALIZED_POPULARITY_DEST, 'w+') as out_tsv:
        dump_selection_to_tsv(DB_CONN_ID, dump_query, POPULARITY_DUMP_DEST)
        log.info('Normalizing popularity data. . .')
        _generate_popularity_tsv(in_tsv, out_tsv, percentiles)
        log.info('Finished normalizing popularity scores. Uploading. . .')
        upload_normalized_popularity(DB_CONN_ID, NORMALIZED_POPULARITY_DEST)
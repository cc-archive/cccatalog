from util.loader import paths, sql
from util.loader.cleanup import clean_image_data


def load_data(output_dir, postgres_conn_id, identifier):
    tsv_file_name = paths.get_staged_file(output_dir, identifier)
    sql.import_data_to_intermediate_table(
        postgres_conn_id,
        tsv_file_name,
        identifier
    )
    clean_image_data(postgres_conn_id, identifier)
    sql.upsert_records_to_image_table(postgres_conn_id, identifier)

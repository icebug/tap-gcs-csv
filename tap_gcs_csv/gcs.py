import re

from google.cloud import storage
from google.oauth2 import service_account

import tap_s3_csv.csv_handler
import tap_s3_csv.excel_handler
from tap_s3_csv.logger import LOGGER as logger

def create_client(config):
    credentials = service_account.Credentials.from_service_account_file(
        config['credentials_path'],
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )
    return storage.Client(credentials=credentials)

def get_row_iterator(config, table_spec, blob):
    file_handle = blob.open()

    if table_spec['format'] == 'csv':
        return tap_s3_csv.csv_handler.get_row_iterator(table_spec, file_handle)

    elif table_spec['format'] == 'excel':
        return tap_s3_csv.excel_handler.get_row_iterator(table_spec, file_handle)


def sample_files(config, table_spec):
    sample_rate = config.get('sample_rate', 10)
    max_records = config.get('max_records', 1000)
    max_files = config.get('max_files', 5)
    for blob in get_files_for_table(config, table_spec, max_results=max_files):
        logger.info('Sampling {} ({} records, every {}th record).'
                    .format(s3_path, max_records, sample_rate))

        iterator = get_row_iterator(config, table_spec, blob)

        samples = 0
        for i, row in enumerate(iterator):
            if (i % sample_rate) == 0:
                yield row
                samples += 1

            if samples >= max_records:
                break

    logger.info('Sampled {} records.'.format(samples))


def get_files_for_table(config, table_spec, max_results=None, last_updated=None):
    client = create_client(config)
    bucket = client.get_bucket(config['bucket'])
    
    pattern = table_spec['pattern']
    matcher = re.compile(pattern)
    prefix = table_spec.get('search_prefix') or re.match(r'[^\(\[\.]+', pattern).group(0)
    logger.debug(
        'Checking bucket "{}" for keys matching "{}"'
        .format(bucket.name, pattern))
    for blob in client.list_blobs(bucket, max_results=max_results, prefix=prefix):
        if matcher.search(blob.name) and (blob.updated is None or blob.updated > last_updated):
            yield blob

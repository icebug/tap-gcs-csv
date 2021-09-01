import argparse
import dateutil
import json
import singer

import tap_gcs_csv.conversion as conversion

import tap_gcs_csv.config
import tap_gcs_csv.gcs

LOGGER = singer.get_logger()

def merge_dicts(first, second):
    to_return = first.copy()

    for key in second:
        if key in first:
            if isinstance(first[key], dict) and isinstance(second[key], dict):
                to_return[key] = merge_dicts(first[key], second[key])
            else:
                to_return[key] = second[key]

        else:
            to_return[key] = second[key]

    return to_return

def get_sampled_schema_for_table(config, table_spec):
    LOGGER.info('Sampling records to determine table schema.')

    samples = [row for row in gcs.sample_files(config, table_spec)]
    data_schema = conversion.generate_schema(samples)

    metadata_schema = {
        '_gcs_source_bucket': {'type': 'string'},
        '_gcs_source_file': {'type': 'string'},
        '_gcs_source_lineno': {'type': 'integer'},
    }

    return {
        'type': 'object',
        'properties': merge_dicts(data_schema, metadata_schema)
    }


def sync_table(config, state, table_spec):
    table_name = table_spec['name']
    last_updated = dateutil.parser.parse(
        state.get(table_name, {}).get('last_updated') or
        config['start_date']
    )

    LOGGER.info('Syncing table "{}".'.format(table_name))
    LOGGER.info('Getting files modified since {}.'.format(last_updated))

    inferred_schema = get_sampled_schema_for_table(config, table_spec)
    override_schema = {'properties': table_spec.get('schema_overrides', {})}
    schema = merge_dicts(inferred_schema, override_schema)
    
    singer.write_schema(table_name, schema, key_properties=table_spec['key_properties'])

    records_streamed = 0
    greatest_last_updated = last_updated
    for blob in gcs.get_files_for_table(config, table_spec, last_updated=last_updated):
        records_streamed += sync_table_file(config, blob, table_spec, schema)

        if blob.updated and blob.updated > greatest_last_updated:
            greatest_last_updated = blob.updated
            state[table_name] = {
                'last_updated': greatest_last_updated.isoformat()
            }
            singer.write_state(state)

    LOGGER.info('Wrote {} records for table "{}".'.format(records_streamed, table_name))

    return state


def sync_table_file(config, blob, table_spec, schema):
    LOGGER.info('Syncing file "{}".'.format(blob.name))

    records_synced = 0

    for row in gcs.row_iterator(config, table_spec, blob):
        metadata = {
            '_gcs_source_bucket': blob.bucket.name,
            '_gcs_source_file': blob.name,
            # index zero, +1 for header row
            '_gcs_source_lineno': records_synced + 2
        }

        record = {**conversion.convert_row(row, schema), **metadata}
        singer.write_record(table_spec['name'], record)
        records_synced += 1

    return records_synced

def load_state(filename):
    state = {}

    if filename is None:
        return state

    try:
        with open(filename) as handle:
            state = json.load(handle)
    except:
        logger.fatal("Failed to decode state file. Is it valid json?")
        raise RuntimeError

    return state

def do_sync(args):
    LOGGER.info('Starting sync.')

    config = tap_gcs_csv.config.load(args.config)
    state = load_state(args.state)

    for table in config['tables']:
        state = sync_table(config, state, table)

    LOGGER.info('Done syncing.')


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-c', '--config', help='Config file', required=True)
    parser.add_argument(
        '-s', '--state', help='State file')

    args = parser.parse_args()

    try:
        do_sync(args)
    except RuntimeError:
        LOGGER.fatal("Run failed.")
        exit(1)


if __name__ == '__main__':
    main()

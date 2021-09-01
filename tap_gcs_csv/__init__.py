import argparse
import dateutil
import json
import singer
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

import tap_gcs_csv.conversion as conversion

from tap_gcs_csv.config import CONFIG_CONTRACT
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


def sync_table(config, state, stream):
    last_updated = dateutil.parser.parse(
        state.get(stream.tap_stream_id, {}).get('last_updated') or
        config['start_date']
    )
    schema = stream.schema.to_dict()
    mdata = singer.metadata.to_map(stream.metadata)
    conversions = {
        key: singer.metadata.get(mdata, ('properties', key), 'conversion_type')
        for key in stream.schema.properties.keys()
    }

    LOGGER.info('Syncing table "{}".'.format(stream.tap_stream_id))
    LOGGER.info('Getting files modified since {}.'.format(last_updated))
    
    singer.write_schema(stream.tap_stream_id, schema, key_properties=stream.key_properties)

    records_streamed = 0
    greatest_last_updated = last_updated
    table_spec = mdata[()]
    for blob in gcs.get_files_for_table(config, table_spec, last_updated=last_updated):
        LOGGER.info('Syncing file "{}".'.format(blob.name))

        for row_id, row in enumerate(gcs.row_iterator(config, table_spec, blob)):
            metadata = {
                '_gcs_source_bucket': blob.bucket.name,
                '_gcs_source_file': blob.name,
                # index zero, +1 for header row
                '_gcs_source_lineno': row_id + 2
            }

            record = {**conversion.convert_row(row, schema, conversions), **metadata}
            singer.write_record(stream.tap_stream_id, record)
            records_streamed += 1

        if blob.updated and blob.updated > greatest_last_updated:
            greatest_last_updated = blob.updated
            state[stream.tap_stream_id] = {
                'last_updated': greatest_last_updated.isoformat()
            }
            singer.write_state(state)

    LOGGER.info('Wrote {} records for table "{}".'.format(records_streamed, stream.tap_stream_id))

    return state


def discover(config):
    streams = []
    for table_spec in config['tables']:
        inferred_schema = get_sampled_schema_for_table(config, table_spec)
        override_schema = {'properties': table_spec.get('schema_overrides', {})}
        merged_schema = merge_dicts(inferred_schema, override_schema)

        schema = Schema.from_dict(merged_schema)
        mdata = singer.metadata.to_map(singer.metadata.get_standard_metadata(
            schema=schema.to_dict(),
            key_properties=table_spec['key_properties']
        ))
        singer.metadata.write(mdata, (), 'selected-by-default', True)
        keys = [
            'search_prefix', 'pattern', 'format', 'encoding', 'compression', 'field_names'
        ]
        for key in keys:
            if key in table_spec:
                singer.metadata.write(mdata, (), key, table_spec[key])

        for prop_name, prop_schema in merged_schema['properties'].items():
            if '_conversion_type' in prop_schema:
                singer.metadata.write(mdata,
                    ('properties', prop_name),
                    'conversion_type',
                    prop_schema['_conversion_type']
                )

        streams.append(
            CatalogEntry(
                tap_stream_id=table_spec['name'],
                stream=table_spec['name'],
                schema=schema,
                key_properties=table_spec['key_properties'],
                metadata=singer.metadata.to_list(mdata),
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)


def do_sync(config, state, catalog):
    LOGGER.info('Starting sync.')

    for stream in catalog.streams:
        selected = stream.is_selected()
        if selected == None:
            selected = True # selected by default
        if selected:
            sync_table(config, state, stream)

    LOGGER.info('Done syncing.')


def main():
    args = singer.utils.parse_args([])

    CONFIG_CONTRACT(args.config)

    if args.discover:
        catalog = discover(args.config)
        catalog.dump()
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover(args.config)

        do_sync(args.config, args.state, catalog)

if __name__ == '__main__':
    main()

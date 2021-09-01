import json
import singer

from voluptuous import Schema, Required, Any, Optional

LOGGER = singer.get_logger()

CONFIG_CONTRACT = Schema({
    Required('credentials_path'): str,
    Required('start_date'): str,
    Required('bucket'): str,
    Required('tables'): [{
        Required('name'): str,
        Required('pattern'): str,
        Required('key_properties'): [str],
        Required('format'): Any('csv', 'excel'),
        Optional('encoding'): str,
        Optional('compression'): Any('none', 'zip'),
        Optional('search_prefix'): str,
        Optional('field_names'): [str],
        Optional('worksheet_name'): str,
        Optional('schema_overrides'): {
            str: {
                Required('type'): Any(str, [str]),
                Required('_conversion_type'): Any('string',
                                                  'integer',
                                                  'number',
                                                  'date-time')
            }
        }
    }]
})


def load(filename):
    config = {}

    try:
        with open(filename) as handle:
            config = json.load(handle)
    except:
        LOGGER.fatal("Failed to decode config file. Is it valid json?")
        raise RuntimeError

    CONFIG_CONTRACT(config)

    return config

from voluptuous import Schema, Required, Any, Optional

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

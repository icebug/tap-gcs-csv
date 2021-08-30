import codecs
import csv

from tap_s3_csv.csv_handler import generator_wrapper

def get_row_iterator(table_spec, file_handle):
    # file_stream = codecs.iterdecode(file_handle, encoding='utf-8', errors='ignore')
    field_names = None

    if 'field_names' in table_spec:
        field_names = table_spec['field_names']

    reader = csv.DictReader(file_handle, fieldnames=field_names)

    return generator_wrapper(reader)

import codecs
import csv
from dataclasses import field


def get_row_iterator(table_spec, file_handle):
    encoding = table_spec.get("encoding", "utf-8")
    field_names = None

    if "field_names" in table_spec:
        field_names = table_spec["field_names"]
    with codecs.getreader(encoding)(file_handle) as file_stream:
        for row in csv.DictReader(
            f=file_stream, dialect="excel", fieldnames=field_names
        ):
            yield row

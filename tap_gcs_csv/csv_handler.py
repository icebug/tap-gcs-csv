import codecs
import csv


def get_row_iterator(table_spec, file_handle):
    encoding = table_spec.get("encoding", "utf-8")
    field_names = None

    if "field_names" in table_spec:
        field_names = table_spec["field_names"]
    with codecs.getreader(encoding)(file_handle) as file_stream:
        dialect = csv.Sniffer().sniff(file_stream.read(1024))
        file_stream.seek(0)
        for row in csv.DictReader(file_stream, dialect=dialect, fieldnames=field_names):
            yield row

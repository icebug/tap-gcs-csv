import codecs
import csv
import singer
from dataclasses import field

LOGGER = singer.get_logger()


ROW_CLEAN_UP_DICT = {
    "inventory_stock_status": {"": "Article"},
    "open_sales": {"": "Article"},
    "sales": {"": "Customer"},
}


def get_row_iterator(table_spec, file_handle, delimiter=None):
    encoding = table_spec.get("encoding", "utf-8")
    field_names = None
    dialect = csv.excel
    if delimiter:
        dialect.delimiter = delimiter

    if "field_names" in table_spec:
        field_names = table_spec["field_names"]
    with codecs.getreader(encoding)(file_handle) as file_stream:

        ### First test of logic, refactor code before finalizing
        if "inventory_stock_status" in table_spec["pattern"]:
            for row in csv.DictReader(
                f=file_stream, dialect=dialect, fieldnames=field_names
            ):
                for k, v in ROW_CLEAN_UP_DICT["inventory_stock_status"].items():
                    row[v] = row.pop(k)
                yield row

        elif "open_sales" in table_spec["pattern"]:
            ARTICLE = ""
            for row in csv.DictReader(
                f=file_stream, dialect=dialect, fieldnames=field_names
            ):
                for k, v in ROW_CLEAN_UP_DICT["open_sales"].items():
                    row[v] = row.pop(k)
                if row["Article"] != None:
                    ARTICLE = row["Article"]
                else:
                    row["Article"] = ARTICLE
                yield row

        elif "sales" in table_spec["pattern"]:
            CUSTOMER = ""
            for row in csv.DictReader(
                f=file_stream, dialect=dialect, fieldnames=field_names
            ):
                for k, v in ROW_CLEAN_UP_DICT["sales"].items():
                    row[v] = row.pop(k)
                if row["Customer"] != None:
                    CUSTOMER = row["Customer"]
                else:
                    row["Customer"] = CUSTOMER
                yield row

        else:
            for row in csv.DictReader(
                f=file_stream, dialect=dialect, fieldnames=field_names
            ):
                yield row

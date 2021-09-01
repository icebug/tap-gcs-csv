import xlrd

def get_row_iterator(table_spec, file_handle):
    workbook = xlrd.open_workbook(
        on_demand=True,
        file_contents=file_handle.read())

    sheet = workbook.sheet_by_name(table_spec["worksheet_name"])
    header_row = None

    for row in reader:
        if header_row is None:
            header_row = row
            continue

        to_return = {}
        for index, cell in enumerate(row):
            header_cell = header_row[index]

            to_return[header_cell.value] = cell.value

        yield to_return

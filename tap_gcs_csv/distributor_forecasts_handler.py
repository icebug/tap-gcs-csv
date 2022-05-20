RELEVANT_COLUMNS = [
    "HONG KONG",
    "JAPAN",
    "CANADA",
    "FINLAND",
    "DENMARK",
    "ESTONIA",
    "POLAND",
    "CZECH REPUBLIC",
    "SLOVENIA and BALKANs",
    "ITALY",
    "SPAIN",
    "SWITZERLAND",
    "BENELUX",
    "UK",
    "ISLAND",
    "USA Wholesale",
    "Germany/ Austria",
    "Sweden/ Norway",
    "Store",
    "Marketplaces (AZ+ ZA)",
    "Webshop (INK US, CAN, FIN)",
]


def forecasts_handler(iterator):
    for row in iterator:
        if row["Article number "] == "":
            continue
        else:
            for col in RELEVANT_COLUMNS:
                try:
                    row[col] = int(row[col].replace(" ", ""))
                except:
                    row[col] = 0
                yield {
                    "Article_number": row["Article number "],
                    "Distribution_ID": col,
                    "Quantity": row[col],
                }

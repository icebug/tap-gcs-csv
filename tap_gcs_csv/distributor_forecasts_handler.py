NUMERICAL_COLUMNS = [
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
    "Webshop per style",
    "TOTAL STOREX",
    "Wholesale STOREX",
    "Retail& Marketplaces",
    "TOTAL Forecast",
    "TOTAL Forecast Per Style",
    "Prod. Forecast to Factory (STO)",
    "TOT Prod Forecast to Factory",
    "Till TW 4 maj 2022",
    "#1 Storex Production",
    "#2 Storex Production",
    "#3 Storex Production",
    "#4 Storex Production",
    "#5 Storex Production",
    "#6 Storex Production",
    "#7 Storex Production",
    "#8 Storex Production",
    "#1 D-warehouse Production",
    "#2 D-warehouse Production",
    "#3 D-warehouse Production",
    "#4 D-warehouse Production",
    "#5 D-warehouse Production",
    "#6 D-warehouse Production",
    "#7 D-warehouse Production",
    "#8 D-warehouse Production",
    "#1 Production Forecast",
    "#2 Production Forecast",
    "#3 Production Forecast",
    "#4 Production Forecast",
    "#5 Production Forecast",
    "#6 Production Forecast",
    "#7 Production Forecast",
    "#8 Production Forecast",
    "TOTAL Prdctn Frcst",
    "TOTAL Prdctn Frcst per style",
    "Diff vs Frcst TOT",
    "LivePO's STO (ex INKPRGNS)",
    "LivePO's D",
    "Unsold product at STOREX last week of season (=stocklist+ left to PreOrder)",
    "Unsold Retail",
    "Unsold Wholesale+ other",
]


def forecasts_handler(iterator, blob, table_spec):
    for row in iterator:
        if row["ROW in CL"] == "":
            continue
        else:
            for col in NUMERICAL_COLUMNS:
                row[col] = float(
                    row[col]
                    .replace(" ", "")
                    .replace("#REF!", "")
                    .replace("#N/A", "")
                    .replace("-", "")
                    .replace("", "0")
                )
            yield row
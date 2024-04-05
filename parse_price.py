import json
import pymysql

"""
{
    "basDt":"20240328",
    "srtnCd":"C01501D94",
    "isinCd":"KR101501D942",
    "itmsNm":"국민주택1종19-04",
    "mrktCtg":"일반채권",
    "xpYrCnt":" ",
    "itmsCtg":" ",
    "clprPrc":"10870",
    "clprVs":"0",
    "clprBnfRt":"3.673",
    "mkpPrc":"10870",
    "mkpBnfRt":"3.673",
    "hiprPrc":"10870",
    "hiprBnfRt":"3.673",
    "loprPrc":"10870",
    "loprBnfRt":"3.673",
    "trqu":"30000",
    "trPrc":"32610"
}
"""

col_name_mapper = {
    "basDt": "base_date",
    "srtnCd": "simple_code",
    "isinCd": "isin_code",
    "itmsNm": "item_name",
    "mrktCtg": "market_category",
    "xpYrCnt": "expire_year_count",
    "itmsCtg": "item_category",
    "clprPrc": "closing_price",
    "clprVs": "closing_price_fluctuation",
    "clprBnfRt": "closing_price_return_rate",
    "mkpPrc": "opening_price",
    "mkpBnfRt": "opening_price_return_rate",
    "hiprPrc": "highest_price",
    "hiprBnfRt": "highest_price_return_rate",
    "loprPrc": "lowest_price",
    "loprBnfRt": "lowest_price_return_rate",
    "trqu": "transaction_quantity",
    "trPrc": "transaction_price_sum"
}


def convert_row(row: dict[str, str]) -> tuple:
    values = []
    for key, val in row.items():
        if val in ('', ' '):
            values.append(None)
        elif any([key.lower().endswith(int_suffix) for int_suffix in ("cnt", "qu")]):
            values.append(int(val))
        elif any([key.lower().endswith(float_suffix) for float_suffix in ("prc", "vs", "rt")]):
            values.append(float(val))
        else:
            values.append(val)
    return tuple(values)


with pymysql.connect(
    host="localhost",
    port=3306,
    user="root",
    password="",
    database="bond"
) as conn:
    with conn.cursor() as cursor:
        sql = "INSERT INTO price VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        for page_no in range(1, 31):
            print(f"page_no: {page_no}")
            with open(f"response/{page_no}.txt", "r") as f:
                data = json.load(f)
                rows = [convert_row(row) for row in data["response"]["body"]["items"]["item"]]
                cursor.executemany(sql, rows)
                # for row in rows:
                #     print(row)
                #     cursor.execute(sql, row)
    conn.commit()
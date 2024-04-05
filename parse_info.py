import json
import pymysql

"""
{
    "basDt":"20200715",
    "crno":"1101110168595",
    "scrsItmsKcd":"1108",
    "isinCd":"KR6010141911",
    "scrsItmsKcdNm":"일반회사채",
    "bondIsurNm":"삼성중공업",
    "isinCdNm":"삼성중공업 101(사모)",
    "bondIssuDt":"20190116",
    "bondIssuFrmtNm":"공사채등록",
    "bondIssuAmt":"10000000000",
    "bondIssuCurCd":"KRW",
    "bondIssuCurCdNm":"KRW",
    "bondExprDt":"20200716",
    "bondPymtAmt":"10000000000",
    "irtChngDcd":"1",
    "irtChngDcdNm":"고정",
    "bondSrfcInrt":"4.2",
    "bondIntTcd":"1",
    "bondIntTcdNm":"이표채"
}
"""


def convert_row(row: dict[str, str]) -> tuple:
    values = []
    for key, val in row.items():
        if val in ('', ' '):
            values.append(None)
        elif any([key.lower().endswith(int_suffix) for int_suffix in ("amt",)]):
            values.append(int(val))
        elif any([key.lower().endswith(float_suffix) for float_suffix in ("rt",)]):
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
        sql = "REPLACE INTO info VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        for page_no in range(1, 6080):
            print(f"page_no: {page_no}")
            with open(f"info/{page_no}.txt", "r") as f:
                data = json.load(f)
                rows = [convert_row(row) for row in data["response"]["body"]["items"]["item"]]
                cursor.executemany(sql, rows)
                # for row in rows:
                #     print(row)
                #     cursor.execute(sql, row)
    conn.commit()
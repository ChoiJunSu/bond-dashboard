from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import pymysql

driver = webdriver.Chrome()

with pymysql.connect(
    host="localhost",
    port=3306,
    user="root",
    password="",
    database="bond"
) as conn:
    conn.autocommit(True)
    with conn.cursor() as cursor:
        cursor.execute("SELECT isin_code FROM info GROUP BY isin_code ORDER BY isin_code")
        isin_codes = [row[0] for row in cursor.fetchall()]
        for idx, isin_code in enumerate(isin_codes):
            print(f"{idx} / {len(isin_codes)}")
            driver.get(f"https://www.hanwhawm.com/main/finance/inbond/FI443_2w.cmd?item_cd={isin_code}")
            time.sleep(0.1)
            rating = driver.find_element(By.ID, 'cgrd_scd').text
            cursor.execute(f"INSERT INTO rating (isin_code, rating) VALUES ('{isin_code}', '{rating}') "
                           f"ON DUPLICATE KEY UPDATE rating = '{rating}'")

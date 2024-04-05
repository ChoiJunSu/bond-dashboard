import requests
import time

url = "https://apis.data.go.kr/1160100/service/GetBondSecuritiesInfoService/getBondPriceInfo"
params = {
    "serviceKey": "LnHeAq9DGmFQILPIjwaL2mawOyEhinqt0asLIDLKFUKBDlWpKwJUj%2FVnez1Rngs8JYVGB5zWslA6zn9I%2FxCVRg%3D%3D",
    "numOfRows": 10000,
    "resultType": "json"
}

for page_no in range(1, 31):
    with open(f"price/{page_no}.txt", "w") as f:
        params["pageNo"] = page_no
        while True:
            res = requests.get(url, params)
            try:
                res.json()
                f.write(res.text)
                break
            except:
                print(f"failed to get page {page_no}... retry in 5 sec")
                time.sleep(5)

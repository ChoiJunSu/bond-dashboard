import requests
import time

url = "https://apis.data.go.kr/1160100/service/GetBondTradInfoService/getIssuIssuItemStat"
params = {
    "serviceKey": "LnHeAq9DGmFQILPIjwaL2mawOyEhinqt0asLIDLKFUKBDlWpKwJUj/Vnez1Rngs8JYVGB5zWslA6zn9I/xCVRg==",
    "numOfRows": 10,
    "resultType": "json"
}

for page_no in range(6080, 6081):
    print(f"page_no: {page_no}")
    with open(f"info/{page_no}.txt", "w") as f:
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

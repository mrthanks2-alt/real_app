import requests
import os
from urllib.parse import unquote

# 사용자가 제공한 정보
service_key = "0b3ec285cec8e3f3f5a04cb624406490416907be4374771617786f413d1fda37"
base_url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev"
operation = "getRTMSDataSvcAptTradeDev"

# 테스트 파라미터 (서산시 2024년 1월)
params = {
    "serviceKey": service_key,
    "LAWD_CD": "44210",
    "DEAL_YMD": "202401",
    "numOfRows": 10,
    "pageNo": 1
}

target_url = f"{base_url}/{operation}"

print(f"Testing URL: {target_url}")
print(f"Params: {params}")

try:
    # 1. 일반적인 방식 (requests가 인코딩하도록 함)
    res = requests.get(target_url, params=params, timeout=10)
    print(f"\n[Test 1] Status Code: {res.status_code}")
    print(f"[Test 1] URL: {res.url}")
    print(f"[Test 1] Response (Partial): {res.text[:500]}")

    # 2. unquote 후 시도
    params["serviceKey"] = unquote(service_key)
    res2 = requests.get(target_url, params=params, timeout=10)
    print(f"\n[Test 2] Status Code: {res2.status_code}")
    print(f"[Test 2] Response (Partial): {res2.text[:500]}")

except Exception as e:
    print(f"\nError occurred: {e}")

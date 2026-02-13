import streamlit as st
import os
import requests
import xmltodict
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time
from urllib.parse import unquote
import json

class RateLimitError(Exception):
    pass

class ApiError(Exception):
    pass

class RTMSClient:
    def __init__(self):
        # 인증키 로드 로직 (보안 강화)
        # 1순위: streamlit.secrets
        # 2순위: os.environ
        self.service_key = None
        
        try:
            if "RTMS_SERVICE_KEY" in st.secrets:
                self.service_key = st.secrets["RTMS_SERVICE_KEY"]
        except:
            pass
            
        if not self.service_key:
            self.service_key = os.environ.get("RTMS_SERVICE_KEY")
            
        if not self.service_key:
            raise ValueError("인증키를 찾을 수 없습니다. Streamlit Secrets 또는 환경변수에 'RTMS_SERVICE_KEY'를 설정해주세요.")

        self.base_url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTradeDev"

    def fetch_monthly_data(self, lawd_cd: str, deal_ymd: str):
        """
        fetches apartment trade data for a specific month.
        deal_ymd: YYYYMM
        """
        # 공백 제거 및 원본 키 사용 (hex 키이므로 별도 디코딩 불필요)
        service_key = self.service_key.strip() if self.service_key else None
        
        params = {
            "serviceKey": service_key,
            "LAWD_CD": lawd_cd,
            "DEAL_YMD": deal_ymd,
            "numOfRows": 9999,
            "pageNo": 1,
            "type": "xml" # 명시적으로 XML 요청
        }
        
        # 상세 자료 API 오퍼레이션 명칭
        target_url = f"{self.base_url}/getRTMSDataSvcAptTradeDev"
        
        try:
            # 타임아웃을 넉넉히 잡고 호출
            response = requests.get(
                target_url, 
                params=params, 
                timeout=(5, 30)
            )
            
            # 디버깅: URL은 따로 저장
            with open("debug_url.txt", "w", encoding="utf-8") as f:
                f.write(response.url)
                
            # 디버깅: XML 전문 저장 (깨끗하게 XML만 저장)
            with open("debug_api.xml", "w", encoding="utf-8") as f:
                f.write(response.text)
                
            response.raise_for_status()
            data = xmltodict.parse(response.text)
            
            # 파싱된 딕셔너리 구조 확인을 위해 저장
            with open("debug_api_data.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                
            header = data.get("response", {}).get("header", {})
            result_code = header.get("resultCode") or header.get("result_code")
            
            if result_code == "000":
                body = data.get("response", {}).get("body", {})
                if not body:
                    return [], result_code
                
                # items가 없을 수도 있음 (데이터가 0건인 경우)
                items_container = body.get("items")
                if not items_container:
                    return [], result_code
                
                items = items_container.get("item")
                if not items:
                    return [], result_code
                    
                res_items = [items] if isinstance(items, dict) else items
                return res_items, result_code
            
            elif result_code == "22":
                raise RateLimitError("API Rate Limit Exceeded")
            
            else:
                msg = header.get("resultMsg", "Unknown Error")
                return [], result_code
                
        except Exception as e:
            # 파싱 실패 시 원본 텍스트 기록을 위해 예외 처리 강화
            with open("debug_error.log", "w", encoding="utf-8") as f:
                f.write(str(e))
            raise ApiError(f"API Error: {e}")

    def get_date_range(self, start_month_str, end_month_str):
        start = datetime.strptime(start_month_str, "%Y%m")
        end = datetime.strptime(end_month_str, "%Y%m")
        curr = start
        res = []
        while curr <= end:
            res.append(curr.strftime("%Y%m"))
            curr += relativedelta(months=1)
        return res

    def process_items(self, items, lawd_cd):
        if not items:
            return pd.DataFrame()
        
        processed = []
        for item in items:
            # 필드명이 대문자인 경우와 한글인 경우 모두 대응 (Gateway API 특성상 다를 수 있음)
            year = item.get("년") or item.get("dealYear") or item.get("DEAL_YEAR")
            month = item.get("월") or item.get("dealMonth") or item.get("DEAL_MONTH")
            day = item.get("일") or item.get("dealDay") or item.get("DEAL_DAY")
            
            # 필수 날짜 정보가 하나라도 없으면 해당 데이터는 건너뜁니다
            if year is None or month is None or day is None:
                continue
                
            try:
                # 금액 정제: "1,200" 또는 숫자 처리
                raw_amount = item.get("거래금액") or item.get("dealAmount") or item.get("DEAL_AMOUNT") or "0"
                amount_str = str(raw_amount).replace(",", "").strip()
                deal_amount = int(amount_str)
                
                # 식별자 추출 보강
                apt_seq = item.get("일련번호") or item.get("aptSeq") or item.get("APT_SEQ") or "unknown"
                apt_nm = item.get("아파트") or item.get("aptNm") or item.get("APT_NM") or "unknown"
                umd_nm = item.get("법정동") or item.get("umdNm") or item.get("UMD_NM") or ""

                processed.append({
                    "lawd_cd": lawd_cd,
                    "deal_ymd": int(f"{int(year)}{int(month):02d}"),
                    "deal_year": int(year),
                    "deal_month": int(month),
                    "deal_day": int(day),
                    "apt_seq": str(apt_seq).strip(),
                    "apt_nm": str(apt_nm).strip(),
                    "umd_nm": str(umd_nm).strip(),
                    "jibun": item.get("지번") or item.get("jibun"),
                    "exclu_use_ar": float(item.get("전용면적") or item.get("excluUseAr") or 0),
                    "deal_amount": deal_amount,
                    "floor": int(item.get("층") or item.get("floor") or 0),
                    "build_year": int(item.get("건축년도") or item.get("buildYear") or 0)
                })
            except:
                # 데이터 형식이 잘못된 경우 건너뜠습니다
                continue
                
        return pd.DataFrame(processed)

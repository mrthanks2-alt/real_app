import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import os
import time

from storage import init_db, save_trades, load_trades, get_last_deal_ymd
from rtms_client import RTMSClient, RateLimitError, ApiError
import analytics

# Page Config
st.set_page_config(page_title="아파트 매매 실거래가 분석 앱", layout="wide")

# Initialize DB
init_db()

def load_region_data():
    # 현재 실행 중인 파일(app.py)의 디렉토리를 기준으로 경로 설정
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base_dir, "lawd_cd.csv")
    
    if not os.path.exists(csv_path):
        return None
    
    # 여러 인코딩 시도 (UTF-8 with BOM, CP949, UTF-8 순서)
    for encoding in ['utf-8-sig', 'cp949', 'utf-8']:
        try:
            df = pd.read_csv(csv_path, dtype=str, encoding=encoding)
            # 컬럼 자동 감지 logic (대소문자 무관하게 code, cd, name, region, 법정동, 지역 포함 여부 확인)
            code_col = next((c for c in df.columns if 'code' in c.lower() or 'cd' in c.lower()), None)
            name_col = next((c for c in df.columns if 'name' in c.lower() or 'region' in c.lower() or '법정동' in c.lower() or '지역' in c.lower()), None)
            
            if code_col and name_col:
                df = df[[name_col, code_col]].rename(columns={name_col: 'name', code_col: 'code'})
                df['code'] = df['code'].str.strip()
                # API에는 앞 5자리 LAWD_CD 사용
                df['lawd_cd'] = df['code'].str[:5]
                return df
        except:
            continue
    return None

# Sidebar
st.sidebar.title("🔍 검색 설정")

region_df = load_region_data()
selected_lawd_cd = None

if region_df is not None:
    region_options = region_df['name'].tolist()
    selected_name = st.sidebar.selectbox("지역 선택", options=region_options)
    selected_lawd_cd = region_df[region_df['name'] == selected_name]['lawd_cd'].values[0]
    st.sidebar.info(f"선택된 법정동 코드: {selected_lawd_cd}")
else:
    st.sidebar.warning("lawd_cd.csv를 찾을 수 없거나 형식이 잘못되었습니다.")
    fallback_cd = st.sidebar.text_input("LAWD_CD 직접 입력 (5자리)", value="11110")
    if len(fallback_cd) == 5:
        selected_lawd_cd = fallback_cd

period_years = st.sidebar.radio("데이터 기간", options=[5, 10], index=0)

st.sidebar.markdown("---")
st.sidebar.subheader("📊 분석 필터")
size_range = st.sidebar.slider("대표평형 밴드 (㎡)", 20.0, 200.0, (84.0, 86.0))
n_total = st.sidebar.number_input("최소 전체 거래수 (N_total)", value=10)
n_85 = st.sidebar.number_input("최소 밴드 거래수 (N_85)", value=5)

btn_update = st.sidebar.button("💾 데이터 적재/갱신")
btn_analyze = st.sidebar.button("📈 분석 실행")

# Common Messages
DISCLAIMER = """
- 실거래 신고/정정 시차 및 누락 가능성으로 결과는 확실하지 않음
- buildYear가 연도 단위인 경우 연식은 근사치(추측입니다)
- 거래빈도는 단지 규모/유동성의 대리변수로 사용(확실하지 않음)
"""

client = RTMSClient()

# Execution Logic
if btn_update:
    if not selected_lawd_cd:
        st.error("지역 코드가 유효하지 않습니다.")
    else:
        with st.spinner("데이터를 가져오는 중..."):
            last_ymd = get_last_deal_ymd(selected_lawd_cd)
            
            end_date = datetime.now()
            if last_ymd:
                start_date = datetime.strptime(str(last_ymd), "%Y%m") + relativedelta(months=1)
                st.info(f"증분 업데이트: {start_date.strftime('%Y-%m')} 부터 데이터 수집")
            else:
                months_back = period_years * 12
                start_date = end_date - relativedelta(months=months_back)
                st.info(f"초기 적재: 최근 {period_years}년 데이터 수집")
            
            # API call range
            date_range = client.get_date_range(start_date.strftime("%Y%m"), end_date.strftime("%Y%m"))
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            total_saved = 0
            total_months = len(date_range)
            
            try:
                for i, ymd in enumerate(date_range):
                    status_text.text(f"🚀 처리 중: {ymd} (지역코드: {selected_lawd_cd}, {i+1}/{total_months})")
                    items, res_code = client.fetch_monthly_data(selected_lawd_cd, ymd)
                    
                    # 데이터 로딩 시도
                    df_step = client.process_items(items, selected_lawd_cd)
                    
                    saved_count = len(df_step)
                    if saved_count > 0:
                        save_trades(df_step)
                        total_saved += saved_count
                        st.write(f"✅ {ymd}: {saved_count}건 저장 완료 (코드: {res_code})")
                    else:
                        st.write(f"⚪ {ymd}: 수집된 데이터 0건 (코드: {res_code})")
                        if i == 0: # 첫 달에 데이터가 없으면 안내 메시지 추가
                            st.caption("팁: 인증키(Service Key)가 '활용 신청' 후 승인 상태인지, '인증키(일반-Decoded)'를 사용 중인지 확인해 보세요.")
                        
                    progress_bar.progress((i + 1) / total_months)
                    time.sleep(0.1) # Soft delay
                
                if total_saved > 0:
                    st.success(f"🎊 데이터 갱신 완료! 총 {total_saved}건 수집됨.")
                else:
                    st.warning("⚠️ 갱신은 완료되었으나, 수집된 데이터가 0건입니다. (API 응답 확인 필요)")
            except RateLimitError as e:
                st.error(str(e))
            except ApiError as e:
                st.error(str(e))
            except Exception as e:
                st.exception(e)

if btn_analyze or 'df_trades' in st.session_state:
    if not selected_lawd_cd:
        st.error("지역을 먼저 선택하세요.")
    else:
        df = load_trades(selected_lawd_cd)
        if df.empty:
            st.warning("표본 부족: 해당 지역의 데이터가 DB에 없습니다. 데이터 적재를 먼저 진행하세요.")
        else:
            df = analytics.add_derived_columns(df)
            st.session_state['df_trades'] = df
            
            # Application of Filters
            df_band = analytics.filter_size_band(df, size_range[0], size_range[1])
            
            # Main UI
            st.title(f"🏠 {selected_name if region_df is not None else selected_lawd_cd} 아파트 실거래 분석")
            
            # KPIs
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("전체 거래 건수", f"{len(df):,}건")
            with col2:
                st.metric("분석 대상(밴드) 건수", f"{len(df_band):,}건")
            with col3:
                curr_median = df_band['pyeong_price_man'].median() if not df_band.empty else 0
                st.metric("평당가 중앙값", f"{curr_median:,.0f}만원")

            # 1. Price/Volume Charts
            trend_data = analytics.compute_trend(df_band)
            if trend_data['monthly'] is not None:
                st.subheader("📈 시세 및 거래량 추세 (선택 평형)")
                
                fig, ax1 = plt.subplots(figsize=(12, 5))
                ax2 = ax1.twinx()
                
                monthly = trend_data['monthly']
                monthly['date'] = monthly['deal_ymd'].apply(lambda x: datetime.strptime(str(x), "%Y%m"))
                
                ax1.plot(monthly['date'], monthly['median_price'], color='blue', marker='o', label='평당가(중앙값)')
                ax2.bar(monthly['date'], monthly['volume'], color='gray', alpha=0.3, width=20, label='거래량')
                
                ax1.set_ylabel("평당가 (만원)", color='blue')
                ax2.set_ylabel("거래량 (건)", color='gray')
                plt.title("월별 평당가 및 거래량 추이")
                st.pyplot(fig)
                
                st.write(f"**단기 모멘텀:** {trend_data['short_momentum_pct']}% | **장기 추세:** {trend_data['long_trend_label']} (기울기: {trend_data['long_slope']})")
                st.caption(trend_data['notes'])

            # 2. Leading Complex
            st.markdown("---")
            st.subheader("🏆 리딩 단지")
            leading = analytics.compute_leading_complex(df, period_years, n_total, n_85, size_range[0], size_range[1])
            
            if leading['top1']:
                c1, c2 = st.columns([1, 2])
                with c1:
                    st.success(f"🥇 Top 1: **{leading['top1']['apt_nm']}**")
                    st.write(f"평당가: {leading['top1']['median_pyeong_price_man']:,.0f}만원")
                    st.write(f"전체거래: {leading['top1']['cnt_total']}건")
                with c2:
                    st.table(leading['top5'][['apt_nm', 'build_year', 'median_pyeong_price_man', 'cnt_total', 'cnt_band']])
                st.caption(leading['notes'])
            else:
                st.info(leading['notes'])

            # 3. Age Group summary
            st.markdown("---")
            st.subheader("🏗️ 연식 구간별 시세 수준")
            age_summary = analytics.compute_age_group_levels(df_band)
            if not age_summary.empty:
                st.dataframe(age_summary, use_container_width=True)
            else:
                st.write("연식 구간 분석을 위한 데이터가 부족합니다.")

            # 4. Raw Data
            st.markdown("---")
            st.subheader("📋 원본 거래 데이터")
            st.dataframe(df.sort_values('deal_ymd', ascending=False), use_container_width=True)

            # Footer
            st.markdown("---")
            st.info(DISCLAIMER)

else:
    st.info("사이드바에서 지역을 선택하고 '분석 실행' 버튼을 눌러주세요.")
    st.markdown(DISCLAIMER)

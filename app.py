import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import time
import platform
try:
    import koreanize_matplotlib
    HAS_KOREANIZE = True
except ImportError:
    HAS_KOREANIZE = False

import matplotlib.pyplot as plt

# 폰트 설정 로직
def set_korean_font():
    plt.rcParams['axes.unicode_minus'] = False # 마이너스 기호 깨짐 방지
    
    if HAS_KOREANIZE:
        # 라이브러리가 있으면 자동 설정 사용
        return
        
    # 라이브러리가 없을 경우 OS별 수동 설정 (Fallback)
    os_name = platform.system()
    if os_name == "Windows":
        plt.rcParams['font.family'] = 'Malgun Gothic'
    elif os_name == "Darwin":
        plt.rcParams['font.family'] = 'AppleGothic'
    else:
        plt.rcParams['font.family'] = 'NanumGothic'

set_korean_font()

from storage import init_db, save_trades, load_trades, get_last_deal_ymd, delete_trades
from rtms_client import RTMSClient, RateLimitError, ApiError
import analytics

# [설정] 컬럼명 한글 매핑 및 단위 명시
COLUMN_MAPPING = {
    'apt_nm': '단지명',
    'deal_year': '년',
    'deal_month': '월',
    'deal_day': '일',
    'exclu_use_ar': '전용면적(㎡)',
    'deal_amount': '거래금액(만원)',
    'floor': '층',
    'build_year': '건축년도',
    'pyeong': '평형',
    'pyeong_price_won': '평당가 (만원)',
    'age': '연식(년)',
    'age_group': '연식구분',
    'cnt': '거래건수',
    'median_pyeong_price_man': '중위 평당가 (만원)',
    'mean_pyeong_price_man': '평균 평당가 (만원)',
    'median_deal_amount': '매매가의 중앙값 (만원)',
    'median_deal_amount_band': '중위 매매가 (만원)',
    'median_pyeong': '전용평형',
    'umd_nm': '법정동',
    'jibun': '지번',
    'cnt_total': '전체 거래수',
    'cnt_band': '밴드 거래수'
}

# 불필요한 시스템 컬럼 목록 (중복 방지를 위해 pyeong_price_man 명시적 삭제)
DROP_COLUMNS = ['lawd_cd', 'deal_ymd', 'apt_seq', 'created_at', 'age_is_estimated', 'pyeong_price_man']

# 정수형 변환이 필요한 컬럼 목록
INT_COLUMNS = [
    'cnt', 'cnt_band', 'cnt_total', 'build_year',
    'median_pyeong_price_man', 'mean_pyeong_price_man',
    'median_deal_amount', 'median_deal_amount_band', 'pyeong_price_won'
]

def format_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """데이터 표시를 위한 전처리(Drop, Cast, Rename)를 수행합니다."""
    if df.empty:
        return df
    
    display_df = df.copy()
    
    # 평당가(원)를 만원 단위로 조정하여 표시 (사용자 요청)
    if 'pyeong_price_won' in display_df.columns:
        display_df['pyeong_price_won'] = display_df['pyeong_price_won'] / 10000
    
    # 1. 불필요 컬럼 삭제
    cols_to_drop = [c for c in DROP_COLUMNS if c in display_df.columns]
    display_df = display_df.drop(columns=cols_to_drop)
    
    # 2. 형 변환
    # 정수형 컬럼 처리 (소수점 제거 및 정수 캐스팅)
    for col in INT_COLUMNS + ['deal_amount']:
        if col in display_df.columns:
            display_df[col] = pd.to_numeric(display_df[col], errors='coerce').fillna(0).astype(int)

    # 실수형 컬럼 처리 (면적, 평형 등 소수점 2자리)
    float_cols = ['exclu_use_ar', 'pyeong', 'median_pyeong']
    for col in float_cols:
        if col in display_df.columns:
            display_df[col] = pd.to_numeric(display_df[col], errors='coerce').round(2)
            
    # 3. 컬럼명 한글화
    display_df = display_df.rename(columns=COLUMN_MAPPING)
    
    return display_df

def style_dataframe(df: pd.DataFrame):
    """데이터프레임에 천 단위 콤마 및 소수점 포맷팅을 적용합니다."""
    # 한글 컬럼명 기준으로 포맷 지정
    format_dict = {
        '거래금액(만원)': '{:,.0f}',
        '평당가 (만원)': '{:,.0f}',
        '중위 평당가 (만원)': '{:,.0f}',
        '평균 평당가 (만원)': '{:,.0f}',
        '매매가의 중앙값 (만원)': '{:,.0f}',
        '중위 매매가 (만원)': '{:,.0f}',
        '거래건수': '{:,.0f}',
        '전체 거래수': '{:,.0f}',
        '밴드 거래수': '{:,.0f}',
        '전용면적(㎡)': '{:,.2f}',
        '평형': '{:,.2f}',
        '전용평형': '{:,.2f}'
    }
    # 실제 존재하는 컬럼에 대해서만 포맷팅 적용
    applied_formats = {k: v for k, v in format_dict.items() if k in df.columns}
    return df.style.format(applied_formats, na_rep="-")

# Page Config
st.set_page_config(page_title="아파트 매매 실거래가 분석 앱", layout="wide")

# Initialize DB
init_db()

@st.cache_data
def load_region_data():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base_dir, "lawd_cd.csv")
    
    if not os.path.exists(csv_path):
        return None
    
    for encoding in ['utf-8-sig', 'cp949', 'utf-8']:
        try:
            df = pd.read_csv(csv_path, dtype=str, encoding=encoding)
            # 컬럼명 유연하게 대응 (region/name, code/cd)
            code_col = next((c for c in df.columns if 'code' in c.lower() or 'cd' in c.lower()), None)
            name_col = next((c for c in df.columns if 'name' in c.lower() or 'region' in c.lower() or '법정동' in c.lower() or '지역' in c.lower()), None)
            
            if code_col and name_col:
                df = df[[name_col, code_col]].rename(columns={name_col: 'region', code_col: 'code'})
                df['code'] = df['code'].str.strip()
                df['lawd_cd'] = df['code'].str[:5]
                
                # 시도/시군구 분리 로직
                df['sido'] = df['region'].apply(lambda x: x.split(' ', 1)[0])
                df['sigungu'] = df['region'].apply(lambda x: x.split(' ', 1)[1] if ' ' in x else "")
                return df
        except:
            continue
    return None

# Sidebar
st.sidebar.title("🔍 검색 설정")

region_df = load_region_data()
selected_lawd_cd = None
selected_name = ""

if region_df is not None:
    # 1단계: 시/도 선택
    sido_list = sorted(region_df['sido'].unique())
    selected_sido = st.sidebar.selectbox("시/도 선택", options=sido_list)
    
    # 2단계: 시/군/구 선택
    sigungu_df = region_df[region_df['sido'] == selected_sido]
    sigungu_list = sorted([s for s in sigungu_df['sigungu'].unique() if s])
    
    if sigungu_list:
        selected_sigungu = st.sidebar.selectbox("시/군/구 선택", options=sigungu_list)
        final_target = sigungu_df[sigungu_df['sigungu'] == selected_sigungu]
    else:
        # 하위 시군구가 없는 경우 (예: 세종시)
        st.sidebar.text("시/군/구 없음 (단일 지역)")
        final_target = sigungu_df
        
    if not final_target.empty:
        selected_lawd_cd = final_target['lawd_cd'].values[0]
        selected_name = final_target['region'].values[0]
        st.sidebar.info(f"선택 지역: {selected_name} ({selected_lawd_cd})")
else:
    st.sidebar.warning("lawd_cd.csv 파일을 찾을 수 없습니다.")
    fallback_cd = st.sidebar.text_input("법정동 코드 직접 입력 (5자리)", value="11110")
    if len(fallback_cd) == 5:
        selected_lawd_cd = fallback_cd
        selected_name = f"코드 {selected_lawd_cd}"

# 데이터 적재 버튼을 선택박스 바로 아래 배치
btn_update = st.sidebar.button("🔄 최신 데이터 가져오기", use_container_width=True, help="선택한 지역의 최신 실거래 데이터를 수집합니다.")

st.sidebar.markdown("---")
st.sidebar.subheader("📊 분석 옵션")
period_years = st.sidebar.radio("조회 기간 선택", options=[3, 5, 10], index=0, help="최근 몇 년간의 데이터를 수집/분석할지 선택합니다.")
size_range = st.sidebar.slider("대표평형 범위 (㎡)", 20.0, 200.0, (84.0, 86.0), help="주요 분석 대상이 될 전용면적 범위를 설정합니다.")
n_total = st.sidebar.number_input("최소 전체 거래건수 (N_total)", value=10, help="단지 선정 시 필요한 최소 전체 거래수입니다.")
n_85 = st.sidebar.number_input("최소 밴드 거래건수 (N_85)", value=5, help="설정한 평형 범위 내에서의 최소 거래수입니다.")

btn_analyze = st.sidebar.button("📈 분석 실행", use_container_width=True)

# Common Messages
DISCLAIMER = """
**[주의사항 및 안내]**
- 실제 거래 신고/정정 시차 및 일부 누락 가능성으로 인해 모든 결과가 완전하지 않을 수 있습니다.
- 건축년도가 연도 단위인 경우, 연식 계산은 당해년도 기준 근사치입니다.
- 거래 빈도는 단지의 규모와 유동성을 가늠하는 지표이며, 절대적인 우위를 보장하지 않습니다.
"""

client = RTMSClient()

# Execution Logic: Data Loading
if btn_update:
    if not selected_lawd_cd:
        st.error("유효한 지역 코드가 없습니다.")
    else:
        with st.spinner("공공데이터포털에서 데이터를 수집 중입니다..."):
            # 1. 기존 데이터 삭제 (사용자 요청: 지역별 강제 재수집)
            delete_trades(selected_lawd_cd)
            
            end_date = datetime.now()
            # 2. 시작일 계산 (오늘 기준 N년 전의 1월 1일)
            start_date = (end_date - relativedelta(months=period_years * 12)).replace(month=1, day=1)
            
            st.info(f"🔄 **전체 재수집**: {selected_name}의 최근 {period_years}년치({start_date.strftime('%Y-%m')} ~) 데이터를 새로 가져옵니다.")
            
            date_range = client.get_date_range(start_date.strftime("%Y%m"), end_date.strftime("%Y%m"))
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            total_saved = 0
            total_months = len(date_range)
            
            try:
                for i, ymd in enumerate(date_range):
                    status_text.text(f"📥 수집 처리 중: {ymd} ({i+1}/{total_months})")
                    items, res_code = client.fetch_monthly_data(selected_lawd_cd, ymd)
                    df_step = client.process_items(items, selected_lawd_cd)
                    
                    saved_count = len(df_step)
                    if saved_count > 0:
                        save_trades(df_step)
                        total_saved += saved_count
                        st.write(f"✅ {ymd}: {saved_count}건 저장 완료")
                    else:
                        st.write(f"⚪ {ymd}: 수집된 데이터 없음")
                        
                    progress_bar.progress((i + 1) / total_months)
                    time.sleep(0.1)
                
                if total_saved > 0:
                    st.success(f"🎊 완료! 총 {total_saved}건의 데이터를 성공적으로 갱신했습니다.")
                else:
                    st.warning("⚠️ 모든 기간을 조회했으나 새로 수집된 데이터가 없습니다.")
            except Exception as e:
                st.error(f"데이터 수집 중 오류 발생: {e}")

# Execution Logic: Analysis
if btn_analyze or 'df_trades' in st.session_state:
    if not selected_lawd_cd:
        st.error("지역을 먼저 선택해 주세요.")
    else:
        df = load_trades(selected_lawd_cd)
        if df.empty:
            st.warning("데이터가 없습니다. 먼저 '데이터 적재/갱신' 버튼을 눌러 데이터를 수집해 주세요.")
        else:
            # 파생 컬럼(평당가 등) 추가
            df = analytics.add_derived_columns(df)

            # [필터링] 1. 선택된 조회 기간(period_years) 필터
            current_year = datetime.now().year
            start_year = current_year - period_years
            df_period = df[df['deal_year'] >= start_year]
            
            # [필터링] 2. 선택된 평형 밴드(size_range) 필터
            df_band = analytics.filter_size_band(df_period, size_range[0], size_range[1])
            
            st.session_state['df_trades'] = df_period # 세션에는 기간 필터 버전 저장
            
            # Main UI - 타이틀 크기 조정 (h3)
            st.markdown(f"<h3>🏠 {selected_name if region_df is not None else selected_lawd_cd} 아파트 실거래 분석</h3>", unsafe_allow_html=True)
            
            # 요약 지표 (KPIs) - 선택된 평형(밴드) 기준
            kpi1, kpi2, kpi3 = st.columns(3)
            with kpi1:
                st.metric("분석 기간 전체 거래", f"{len(df_period):,}건")
            with kpi2:
                st.metric("선택 평형(밴드) 거래", f"{len(df_band):,}건")
            with kpi3:
                # pyeong_price_won(원 단위)을 10,000으로 나누어 만원 단위로 계산
                curr_median = (df_band['pyeong_price_won'].median() / 10000) if not df_band.empty else 0
                st.metric("선택 평형 중위 평당가", f"{curr_median:,.0f}만원")

            # 1. 시세 및 거래량 추세 차트 (반드시 밴드 데이터만 사용)
            st.markdown("---")
            trend_data = analytics.compute_trend(df_band)
            if trend_data['monthly'] is not None:
                # 소제목 크기 조정 (h5)
                st.markdown("<h5>📈 시세 및 거래량 추세 (선택 평형 대상)</h5>", unsafe_allow_html=True)
                
                fig, ax1 = plt.subplots(figsize=(12, 5))
                ax2 = ax1.twinx()
                
                monthly = trend_data['monthly']
                monthly['date'] = monthly['deal_ymd'].apply(lambda x: datetime.strptime(str(x), "%Y%m"))
                
                ax1.plot(monthly['date'], monthly['median_price'], color='#1f77b4', marker='o', linewidth=2, label='평당가(중앙값)')
                ax2.bar(monthly['date'], monthly['volume'], color='#d62728', alpha=0.3, width=20, label='거래량')
                
                ax1.set_xlabel("거래 시점", fontsize=10)
                ax1.set_ylabel("평당가 (만원)", color='#1f77b4', fontsize=10)
                ax2.set_ylabel("거래량 (건)", color='#d62728', fontsize=10)
                ax1.grid(True, axis='y', linestyle='--', alpha=0.6)
                
                plt.title(f"월별 평당가 및 거래량 추이 ({selected_name})", fontsize=14, pad=20)
                
                # 범례 통합 표시
                lines, labels = ax1.get_legend_handles_labels()
                bars, labels2 = ax2.get_legend_handles_labels()
                ax1.legend(lines + bars, labels + labels2, loc='upper left')
                
                st.pyplot(fig)
                
                st.write(f"🔍 **단기 모멘텀:** {trend_data['short_momentum_pct']}% | **장기 추세:** {trend_data['long_trend_label']} (기울기: {trend_data['long_slope']})")
                st.caption(f"※ {trend_data['notes']}")

            # 2. 리딩 단지 분석
            st.markdown("---")
            st.markdown("<h5>🏆 지역 리딩 단지 (대장주)</h5>", unsafe_allow_html=True)
            leading = analytics.compute_leading_complex(df_period, period_years, n_total, n_85, size_range[0], size_range[1])
            
            if leading['top1']:
                c1, c2 = st.columns([1, 2])
                with c1:
                    st.success(f"✨ **지역 핵심 단지**: {leading['top1']['apt_nm']}")
                    st.info(f"""
                    - **중위 평당가**: {int(leading['top1']['median_pyeong_price_man']):,}만원
                    - **건축년도**: {int(leading['top1']['build_year'])}년
                    - **{period_years}년간 전체 거래**: {int(leading['top1']['cnt_total'])}건
                    """)
                with c2:
                    st.markdown("<b>상위 5개 단지 상세</b>", unsafe_allow_html=True)
                    display_top5 = format_for_display(leading['top5'])
                    # 컬럼 순서 조정: ["아파트명", "전용평형", "중위 평당가 (만원)", "중위 매매가 (만원)", "전체 거래수"]
                    # 단지명(apt_nm)을 아파트명으로 표시하기 위해 매핑 확인
                    display_top5 = display_top5.rename(columns={'단지명': '아파트명'})
                    cols_to_show = ['아파트명', '건축년도', '전용평형', '중위 평당가 (만원)', '중위 매매가 (만원)', '전체 거래수', '밴드 거래수']
                    st.table(style_dataframe(display_top5[[c for c in cols_to_show if c in display_top5.columns]]))
                st.caption(f"💡 {leading['notes']}")
            else:
                st.info(f"ℹ️ {leading['notes']}")

            # 3. 연식 구간별 분석
            st.markdown("---")
            st.markdown("<h5>🏗️ 연식 구간별 시세 수준</h5>", unsafe_allow_html=True)
            age_summary = analytics.compute_age_group_levels(df_band)
            if not age_summary.empty:
                display_age = format_for_display(age_summary)
                st.dataframe(style_dataframe(display_age), use_container_width=True, hide_index=True)
            else:
                st.info("연식 구분을 위한 데이터가 충분하지 않습니다.")

            # 4. 원본 거래 데이터 (선택 평형 기준)
            st.markdown("---")
            st.markdown("<h5>📋 선택 평형 실거래 내역</h5>", unsafe_allow_html=True)
            # 내림차순 정렬 후 표시 전처리 적용
            display_raw = format_for_display(df_band.sort_values(['deal_year', 'deal_month', 'deal_day'], ascending=False))
            st.dataframe(style_dataframe(display_raw), use_container_width=True, hide_index=True)

            # Footer
            st.markdown("---")
            st.info(DISCLAIMER)
else:
    st.info("왼쪽 사이드바에서 지역을 선택한 후 **[📈 분석 실행]** 버튼을 눌러주세요.")
    st.markdown(DISCLAIMER)

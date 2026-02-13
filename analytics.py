import pandas as pd
import numpy as np
from datetime import datetime

def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    
    # 1. pyeong: 전용면적 / 3.30578
    df['pyeong'] = df['exclu_use_ar'] / 3.30578
    
    # 2. price calculations
    df['pyeong_price_won'] = (df['deal_amount'] * 10000) / df['pyeong']
    df['pyeong_price_man'] = df['deal_amount'] / df['pyeong']
    
    # 3. age: current_year - build_year
    current_year = datetime.now().year
    df['age'] = current_year - df['build_year']
    df['age_is_estimated'] = True # As per requirement "추측입니다"
    
    return df

def filter_size_band(df: pd.DataFrame, min_m2: float, max_m2: float) -> pd.DataFrame:
    return df[(df['exclu_use_ar'] >= min_m2) & (df['exclu_use_ar'] <= max_m2)]

def compute_leading_complex(df: pd.DataFrame, lookback_years: int, n_total: int, n_85: int, min_m2: float, max_m2: float) -> dict:
    if df.empty:
        return {"top1": None, "top5": None, "params": locals(), "notes": "데이터가 없습니다."}
    
    # 1. 선정 전용 데이터셋 (기간 필터)
    current_year = datetime.now().year
    df_recent = df[df['deal_year'] >= (current_year - lookback_years)]
    
    # 2. 전체 거래수 집합 (모든 평형 합산)
    total_stats = df_recent.groupby(['apt_seq', 'apt_nm']).agg(
        build_year=('build_year', 'max'),
        cnt_total=('deal_amount', 'count')
    ).reset_index()
    
    # 3. 밴드 내 통계 집합 (설정 평형 한정: 가격 및 건수)
    df_band = filter_size_band(df_recent, min_m2, max_m2)
    band_stats = df_band.groupby(['apt_seq', 'apt_nm']).agg(
        cnt_band=('deal_amount', 'count'),
        median_pyeong_price_man=('pyeong_price_man', 'median'),
        median_pyeong=('pyeong', 'median')
    ).reset_index()
    
    # 밴드 내 중위 매매가 계산 (평형 * 중위평당가)
    band_stats['median_deal_amount_band'] = band_stats['median_pyeong'] * band_stats['median_pyeong_price_man']
    
    # 4. 두 집합 결합 (밴드 내 거래가 있는 단지 기준)
    grouped = pd.merge(total_stats, band_stats, on=['apt_seq', 'apt_nm'], how='inner')
    
    # 5. 최종 필터링: 전체 거래수 >= n_total AND 밴드 거래수 >= n_85
    filtered = grouped[(grouped['cnt_total'] >= n_total) & (grouped['cnt_band'] >= n_85)]
    
    if filtered.empty:
        return {"top1": None, "top5": None, "params": locals(), "notes": "조건을 만족하는 단지가 없습니다."}
    
    # Sort by median price
    sorted_df = filtered.sort_values(by='median_pyeong_price_man', ascending=False)
    
    top1 = sorted_df.iloc[0].to_dict()
    top5 = sorted_df.head(5)
    
    return {
        "top1": top1,
        "top5": top5,
        "params": {"lookback_years": lookback_years, "n_total": n_total, "n_85": n_85, "min_m2": min_m2, "max_m2": max_m2},
        "notes": "거래빈도는 단지 규모 대리변수이므로 결과에 확실하지 않음 문구 포함"
    }

def compute_age_group_levels(df_band: pd.DataFrame, min_samples: int = 10) -> pd.DataFrame:
    if df_band.empty:
        return pd.DataFrame()
    
    # Define age groups
    def get_age_group(age):
        if age <= 5: return "신축(5년이내)"
        elif age <= 10: return "준신축(5~10년)"
        else: return "구축(10년이상)"
    
    df_band = df_band.copy()
    df_band['age_group'] = df_band['age'].apply(get_age_group)
    
    # 정해진 순서대로 정렬하기 위해 Categorical 데이터 처리
    order = ["신축(5년이내)", "준신축(5~10년)", "구축(10년이상)"]
    df_band['age_group'] = pd.Categorical(df_band['age_group'], categories=order, ordered=True)
    
    summary = df_band.groupby('age_group', observed=True).agg(
        median_pyeong_price_man=('pyeong_price_man', 'median'),
        mean_pyeong_price_man=('pyeong_price_man', 'mean'),
        median_deal_amount=('deal_amount', 'median'),
        cnt=('deal_amount', 'count')
    ).reset_index()
    
    summary['is_uncertain'] = summary['cnt'] < min_samples
    
    return summary

def compute_trend(df_band: pd.DataFrame) -> dict:
    if df_band.empty:
        return {"monthly": None, "short_momentum_pct": 0, "long_slope": 0, "long_trend_label": "데이터 부족", "notes": ""}
    
    # Monthly aggregation
    monthly = df_band.groupby('deal_ymd').agg(
        median_price=('pyeong_price_man', 'median'),
        volume=('deal_amount', 'count')
    ).reset_index().sort_values('deal_ymd')
    
    if len(monthly) < 2:
        return {"monthly": monthly, "short_momentum_pct": 0, "long_slope": 0, "long_trend_label": "데이터 부족", "notes": "분석을 위한 데이터가 부족합니다."}
    
    # Short momentum: Last 2 available months
    recent_2 = monthly.tail(2)
    short_momentum_pct = ((recent_2.iloc[-1]['median_price'] / recent_2.iloc[0]['median_price']) - 1) * 100 if len(recent_2) == 2 else 0
    
    # Long trend: Last 36 months linear regression
    last_36 = monthly.tail(36)
    x = np.arange(len(last_36))
    y = last_36['median_price'].values
    
    if len(last_36) >= 12: # Minimum 12 months for "long" trend
        slope, _ = np.polyfit(x, y, 1)
        long_trend_label = "상승" if slope > 0 else "하락"
    else:
        slope = 0
        long_trend_label = "데이터 부족"
        
    return {
        "monthly": monthly,
        "short_momentum_pct": round(short_momentum_pct, 2),
        "long_slope": round(float(slope), 2),
        "long_trend_label": long_trend_label,
        "notes": "장기 추세는 최근 36개월 선형회귀 slope 기반입니다."
    }

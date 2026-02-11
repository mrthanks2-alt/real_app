import sqlite3
import pandas as pd
from datetime import datetime
import os

DB_NAME = "rtms_trades.sqlite"

def get_connection():
    return sqlite3.connect(DB_NAME)

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS trade_raw (
            lawd_cd TEXT,
            deal_ymd INTEGER,
            deal_year INTEGER,
            deal_month INTEGER,
            deal_day INTEGER,
            apt_seq TEXT,
            apt_nm TEXT,
            umd_nm TEXT,
            jibun TEXT,
            exclu_use_ar REAL,
            deal_amount INTEGER,
            floor INTEGER,
            build_year INTEGER,
            created_at TEXT,
            UNIQUE(apt_seq, deal_year, deal_month, deal_day, exclu_use_ar, floor, deal_amount)
        )
    """)
    conn.commit()
    conn.close()

def save_trades(df: pd.DataFrame):
    if df.empty:
        return
    
    conn = get_connection()
    # Adding created_at
    df['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # We use INSERT OR IGNORE as required
    columns = [
        'lawd_cd', 'deal_ymd', 'deal_year', 'deal_month', 'deal_day',
        'apt_seq', 'apt_nm', 'umd_nm', 'jibun', 'exclu_use_ar',
        'deal_amount', 'floor', 'build_year', 'created_at'
    ]
    
    # Filter only required columns if exists
    df_to_save = df[[c for c in columns if c in df.columns]]
    
    placeholders = ", ".join(["?"] * len(df_to_save.columns))
    cols_str = ", ".join(df_to_save.columns)
    sql = f"INSERT OR IGNORE INTO trade_raw ({cols_str}) VALUES ({placeholders})"
    
    conn.executemany(sql, df_to_save.values.tolist())
    conn.commit()
    conn.close()

def get_last_deal_ymd(lawd_cd: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(deal_ymd) FROM trade_raw WHERE lawd_cd = ?", (lawd_cd,))
    result = cursor.fetchone()[0]
    conn.close()
    return result

def load_trades(lawd_cd: str) -> pd.DataFrame:
    conn = get_connection()
    query = "SELECT * FROM trade_raw WHERE lawd_cd = ?"
    df = pd.read_sql_query(query, conn, params=(lawd_cd,))
    conn.close()
    return df

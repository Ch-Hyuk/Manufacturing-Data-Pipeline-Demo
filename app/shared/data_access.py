from __future__ import annotations

from pathlib import Path

import pandas as pd
import psycopg2
import streamlit as st

from shared.config import NUMERIC_COLUMNS, RAW_FILES, RAW_TABLES
import os


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "manufacturing_dw"),
        user=os.getenv("POSTGRES_USER", "manufacturing"),
        password=os.getenv("POSTGRES_PASSWORD", "manufacturing"),
    )


@st.cache_data(ttl=10)
def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


@st.cache_data(ttl=10)
def query_table(table_name: str, limit: int = 500) -> pd.DataFrame:
    try:
        with get_connection() as conn:
            df = pd.read_sql_query(f"select * from {table_name} limit {limit}", conn)
            for column in NUMERIC_COLUMNS.intersection(df.columns):
                df[column] = pd.to_numeric(df[column], errors="coerce")
            return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=10)
def table_count(table_name: str) -> int:
    try:
        with get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"select count(*) from {table_name}")
                return int(cursor.fetchone()[0])
    except Exception:
        return 0


def file_row_count(path: Path) -> int:
    return len(read_csv(path))


def raw_files_ready() -> bool:
    return all(path.exists() and file_row_count(path) > 0 for path in RAW_FILES.values())


def raw_tables_ready() -> bool:
    return all(table_count(table) > 0 for table in RAW_TABLES)


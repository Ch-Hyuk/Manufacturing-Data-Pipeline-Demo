from __future__ import annotations

import pandas as pd


def normalize_date_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    if df.empty or column not in df.columns:
        return df
    df = df.copy()
    df[column] = pd.to_datetime(df[column], errors="coerce").dt.date
    return df


def normalize_datetime_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    if df.empty or column not in df.columns:
        return df
    df = df.copy()
    df[column] = pd.to_datetime(df[column], errors="coerce")
    return df


def sorted_unique(df: pd.DataFrame, column: str) -> list[str]:
    if df.empty or column not in df.columns:
        return []
    values = df[column].dropna().astype(str).unique().tolist()
    return sorted(values)


def filter_by_values(df: pd.DataFrame, column: str, selected: list[str]) -> pd.DataFrame:
    if df.empty or column not in df.columns or not selected:
        return df
    return df[df[column].astype(str).isin(selected)]


def filter_by_date_range(df: pd.DataFrame, column: str, date_range: tuple | list) -> pd.DataFrame:
    if df.empty or column not in df.columns or not date_range or len(date_range) != 2:
        return df
    start_date, end_date = date_range
    return df[(df[column] >= start_date) & (df[column] <= end_date)]


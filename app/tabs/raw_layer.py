from __future__ import annotations

import streamlit as st

from shared.config import RAW_FILES, RAW_TABLES
from shared.data_access import read_csv, table_count


def render_raw_tab() -> None:
    st.subheader("Raw CSV Files")
    for label, path in RAW_FILES.items():
        df = read_csv(path)
        with st.expander(f"{label} - {len(df):,} rows", expanded=label == "Sensor MQTT CSV"):
            st.dataframe(df.tail(200), use_container_width=True)

    st.subheader("PostgreSQL Raw Tables")
    cols = st.columns(3)
    for idx, table in enumerate(RAW_TABLES):
        cols[idx].metric(table, f"{table_count(table):,} rows")


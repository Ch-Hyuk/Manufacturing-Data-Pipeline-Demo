from __future__ import annotations

import streamlit as st

from tabs.data_mart import render_data_mart_tab
from tabs.flow import render_flow_tab
from tabs.raw_layer import render_raw_tab
from tabs.telemetry import render_telemetry_tab


st.set_page_config(page_title="Manufacturing Flow Viewer", layout="wide")

st.title("Manufacturing Data Pipeline Flow Viewer")
st.caption("Monitor virtual machines, MQTT telemetry, Airflow pipeline outputs, and data mart KPIs.")

overview, telemetry, raw, mart = st.tabs(["Flow", "Telemetry", "Raw Layer", "Data Mart"])

with overview:
    render_flow_tab()

with telemetry:
    render_telemetry_tab()

with raw:
    render_raw_tab()

with mart:
    render_data_mart_tab()


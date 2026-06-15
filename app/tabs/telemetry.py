from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from shared.config import RAW_FILES
from shared.data_access import read_csv


def render_telemetry_tab() -> None:
    sensor = read_csv(RAW_FILES["Sensor MQTT CSV"])
    st.subheader("Latest MQTT Telemetry")
    if sensor.empty:
        st.info("No MQTT sensor data has been collected yet. Start the simulator and run the Airflow DAG.")
        return

    st.dataframe(sensor.tail(100), use_container_width=True)

    if {"event_time", "machine_id", "temperature", "pressure", "vibration"}.issubset(sensor.columns):
        sensor = sensor.copy()
        sensor["event_time"] = pd.to_datetime(sensor["event_time"], errors="coerce")
        machine = st.selectbox("Machine", sorted(sensor["machine_id"].dropna().unique()))
        filtered = sensor[sensor["machine_id"] == machine].sort_values("event_time")

        metric = st.radio("Sensor Metric", ["temperature", "pressure", "vibration"], horizontal=True)
        fig = px.line(filtered, x="event_time", y=metric, color="machine_id")
        st.plotly_chart(fig, use_container_width=True)


from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from shared.data_access import query_table
from shared.transforms import filter_by_date_range, filter_by_values, normalize_date_column, normalize_datetime_column, sorted_unique


def get_filtered_mart_data() -> dict[str, pd.DataFrame | list[str] | tuple]:
    production = normalize_date_column(query_table("dm_daily_production", limit=5000), "production_date")
    quality = normalize_date_column(query_table("dm_daily_quality", limit=5000), "production_date")
    health = normalize_date_column(query_table("dm_machine_health", limit=5000), "event_date")
    raw_sensor = normalize_datetime_column(query_table("raw_sensor_data", limit=10000), "event_time")
    raw_production = normalize_date_column(query_table("raw_production_data", limit=10000), "production_date")
    raw_quality = normalize_datetime_column(query_table("raw_quality_data", limit=10000), "inspection_time")

    machine_options = sorted(set(sorted_unique(production, "machine_id") + sorted_unique(health, "machine_id") + sorted_unique(raw_sensor, "machine_id")))
    product_options = sorted(set(sorted_unique(production, "product_id") + sorted_unique(raw_production, "product_id")))
    status_options = sorted_unique(health, "health_status")

    all_dates: list = []
    for df, column in [(production, "production_date"), (quality, "production_date"), (health, "event_date")]:
        if not df.empty and column in df.columns:
            all_dates.extend(df[column].dropna().tolist())

    if all_dates:
        min_date = min(all_dates)
        max_date = max(all_dates)
    else:
        today = pd.Timestamp.now().date()
        min_date = today
        max_date = today

    with st.container(border=True):
        st.subheader("Analysis Scope")
        goal = st.selectbox(
            "Purpose",
            [
                "Operations overview",
                "Production by machine",
                "Defect rate analysis",
                "Current machine status",
                "Anomaly detection",
                "Raw detail exploration",
            ],
        )
        col1, col2, col3, col4 = st.columns(4)
        selected_dates = col1.date_input("Date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
        selected_machines = col2.multiselect("Machine", machine_options, default=machine_options)
        selected_products = col3.multiselect("Product", product_options, default=product_options)
        selected_statuses = col4.multiselect("Health Status", status_options, default=status_options)

    filtered_production = filter_by_date_range(production, "production_date", selected_dates)
    filtered_production = filter_by_values(filtered_production, "machine_id", selected_machines)
    filtered_production = filter_by_values(filtered_production, "product_id", selected_products)

    filtered_quality = filter_by_date_range(quality, "production_date", selected_dates)
    filtered_quality = filter_by_values(filtered_quality, "machine_id", selected_machines)

    filtered_health = filter_by_date_range(health, "event_date", selected_dates)
    filtered_health = filter_by_values(filtered_health, "machine_id", selected_machines)
    filtered_health = filter_by_values(filtered_health, "health_status", selected_statuses)

    filtered_sensor = filter_by_values(raw_sensor, "machine_id", selected_machines)
    filtered_sensor = filter_by_values(filtered_sensor, "product_id", selected_products)

    filtered_raw_production = filter_by_date_range(raw_production, "production_date", selected_dates)
    filtered_raw_production = filter_by_values(filtered_raw_production, "machine_id", selected_machines)
    filtered_raw_production = filter_by_values(filtered_raw_production, "product_id", selected_products)

    if not raw_quality.empty and not filtered_raw_production.empty and "lot_id" in raw_quality.columns:
        filtered_raw_quality = raw_quality[raw_quality["lot_id"].isin(filtered_raw_production["lot_id"])]
    else:
        filtered_raw_quality = raw_quality

    return {
        "goal": goal,
        "production": filtered_production,
        "quality": filtered_quality,
        "health": filtered_health,
        "sensor": filtered_sensor,
        "raw_production": filtered_raw_production,
        "raw_quality": filtered_raw_quality,
    }


def render_analysis_kpis(production: pd.DataFrame, quality: pd.DataFrame, health: pd.DataFrame, sensor: pd.DataFrame) -> None:
    latest_sensor = sensor.sort_values("event_time").groupby("machine_id").tail(1) if not sensor.empty and "event_time" in sensor.columns else pd.DataFrame()
    anomaly_count = int(sensor["anomaly_type"].notna().sum()) if not sensor.empty and "anomaly_type" in sensor.columns else 0
    warning_or_danger = int(health["health_status"].isin(["WARNING", "DANGER"]).sum()) if not health.empty and "health_status" in health.columns else 0

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Selected Production", f"{int(production['total_quantity'].sum()) if not production.empty else 0:,}")
    col2.metric("Avg Defect Rate", f"{quality['defect_rate'].mean() * 100:.2f}%" if not quality.empty else "0.00%")
    col3.metric("Active Machines", f"{latest_sensor['machine_id'].nunique() if not latest_sensor.empty else 0:,}")
    col4.metric("Warning/Danger Days", f"{warning_or_danger:,}")
    col5.metric("Sensor Anomalies", f"{anomaly_count:,}")


def render_current_machine_status(sensor: pd.DataFrame, health: pd.DataFrame) -> None:
    st.subheader("Current Machine Status")
    if sensor.empty:
        st.info("No sensor telemetry is available for the selected scope.")
        return

    latest = sensor.sort_values("event_time").groupby("machine_id").tail(1)
    if not health.empty:
        latest_health = health.sort_values("event_date").groupby("machine_id").tail(1)[["machine_id", "health_status"]]
        latest = latest.merge(latest_health, on="machine_id", how="left")

    status_cols = ["machine_id", "line_id", "product_id", "mode", "health_status", "temperature", "pressure", "vibration", "motor_current", "rpm", "anomaly_type", "event_time"]
    available_cols = [column for column in status_cols if column in latest.columns]
    st.dataframe(latest[available_cols].sort_values("machine_id"), use_container_width=True)

    metric_cols = st.columns(3)
    for idx, metric in enumerate(["temperature", "pressure", "vibration"]):
        if metric in latest.columns:
            fig = px.bar(latest, x="machine_id", y=metric, color="health_status" if "health_status" in latest.columns else "machine_id")
            metric_cols[idx].plotly_chart(fig, use_container_width=True)


def render_production_analysis(production: pd.DataFrame, raw_production: pd.DataFrame) -> None:
    st.subheader("Production Analysis")
    if production.empty:
        st.info("No production mart data is available for the selected scope.")
        return

    left, right = st.columns(2)
    with left:
        by_machine = production.groupby("machine_id", as_index=False)["total_quantity"].sum()
        fig = px.bar(by_machine, x="machine_id", y="total_quantity", color="machine_id")
        st.plotly_chart(fig, use_container_width=True)
    with right:
        by_product = production.groupby("product_id", as_index=False)["total_quantity"].sum()
        fig = px.pie(by_product, names="product_id", values="total_quantity", hole=0.45)
        st.plotly_chart(fig, use_container_width=True)

    if not raw_production.empty and {"planned_quantity", "quantity", "machine_id"}.issubset(raw_production.columns):
        raw_production = raw_production.copy()
        raw_production["achievement_rate"] = raw_production["quantity"] / raw_production["planned_quantity"]
        fig = px.box(raw_production, x="machine_id", y="achievement_rate", color="machine_id", points="all")
        st.plotly_chart(fig, use_container_width=True)


def render_quality_analysis(quality: pd.DataFrame, raw_quality: pd.DataFrame, raw_production: pd.DataFrame) -> None:
    st.subheader("Defect Rate Analysis")
    if quality.empty:
        st.info("No quality mart data is available for the selected scope.")
        return

    left, right = st.columns(2)
    with left:
        fig = px.bar(quality, x="machine_id", y="defect_rate", color="machine_id", hover_data=["total_lot_count", "defect_lot_count"])
        st.plotly_chart(fig, use_container_width=True)
    with right:
        fig = px.scatter(quality, x="total_lot_count", y="defect_rate", size="defect_lot_count", color="machine_id")
        st.plotly_chart(fig, use_container_width=True)

    if not raw_quality.empty and not raw_production.empty:
        joined = raw_quality.merge(raw_production[["lot_id", "machine_id", "product_id", "shift"]], on="lot_id", how="left")
        failed = joined[joined["result"] == "FAIL"] if "result" in joined.columns else pd.DataFrame()
        if not failed.empty and "defect_type" in failed.columns:
            defect_mix = failed.groupby(["defect_type", "machine_id"], dropna=False).size().reset_index(name="defect_count")
            fig = px.bar(defect_mix, x="defect_type", y="defect_count", color="machine_id", barmode="group")
            st.plotly_chart(fig, use_container_width=True)
            st.dataframe(failed.sort_values("inspection_time", ascending=False).head(200), use_container_width=True)


def render_anomaly_analysis(sensor: pd.DataFrame, health: pd.DataFrame) -> None:
    st.subheader("Anomaly Detection")
    anomaly_sensor = sensor[sensor["anomaly_type"].notna()] if not sensor.empty and "anomaly_type" in sensor.columns else pd.DataFrame()
    risk_health = health[health["health_status"].isin(["WARNING", "DANGER"])] if not health.empty and "health_status" in health.columns else pd.DataFrame()

    col1, col2 = st.columns(2)
    col1.metric("Telemetry Anomaly Rows", f"{len(anomaly_sensor):,}")
    col2.metric("Warning/Danger Machine Days", f"{len(risk_health):,}")

    if not anomaly_sensor.empty:
        anomaly_mix = anomaly_sensor.groupby(["machine_id", "anomaly_type"]).size().reset_index(name="events")
        fig = px.bar(anomaly_mix, x="machine_id", y="events", color="anomaly_type", barmode="stack")
        st.plotly_chart(fig, use_container_width=True)

        sensor = sensor.copy()
        sensor["event_time"] = pd.to_datetime(sensor["event_time"], errors="coerce")
        selected_machine = st.selectbox("Anomaly trend machine", sorted(anomaly_sensor["machine_id"].dropna().unique()))
        machine_sensor = sensor[sensor["machine_id"] == selected_machine].sort_values("event_time")
        fig = px.line(machine_sensor, x="event_time", y=["temperature", "pressure", "vibration"])
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(anomaly_sensor.sort_values("event_time", ascending=False).head(200), use_container_width=True)
    elif not risk_health.empty:
        fig = px.scatter(risk_health, x="avg_temperature", y="avg_vibration", color="health_status", size="avg_pressure", hover_data=["machine_id", "event_date"])
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(risk_health, use_container_width=True)
    else:
        st.success("No anomaly or warning/danger machine days found in the selected scope.")


def render_data_mart_tab() -> None:
    data = get_filtered_mart_data()
    production = data["production"]
    quality = data["quality"]
    health = data["health"]
    sensor = data["sensor"]
    raw_production = data["raw_production"]
    raw_quality = data["raw_quality"]

    if production.empty and quality.empty and health.empty:
        st.info("Data mart is not ready yet. Run the Airflow DAG first.")
        return

    render_analysis_kpis(production, quality, health, sensor)

    goal = data["goal"]
    if goal == "Operations overview":
        render_current_machine_status(sensor, health)
        render_production_analysis(production, raw_production)
        render_quality_analysis(quality, raw_quality, raw_production)
    elif goal == "Production by machine":
        render_production_analysis(production, raw_production)
    elif goal == "Defect rate analysis":
        render_quality_analysis(quality, raw_quality, raw_production)
    elif goal == "Current machine status":
        render_current_machine_status(sensor, health)
    elif goal == "Anomaly detection":
        render_anomaly_analysis(sensor, health)
    else:
        st.subheader("Selected Data")
        dataset = st.selectbox("Dataset", ["Production Mart", "Quality Mart", "Machine Health Mart", "Raw Sensor", "Raw Production", "Raw Quality"])
        frames = {
            "Production Mart": production,
            "Quality Mart": quality,
            "Machine Health Mart": health,
            "Raw Sensor": sensor,
            "Raw Production": raw_production,
            "Raw Quality": raw_quality,
        }
        st.dataframe(frames[dataset], use_container_width=True)


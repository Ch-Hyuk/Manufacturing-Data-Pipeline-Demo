from __future__ import annotations

import json
import os
import socket
import time
from pathlib import Path

import paho.mqtt.client as mqtt
import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st


DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
RAW_DIR = DATA_DIR / "raw"
MQTT_CONTROL_TOPIC = "manufacturing/control/simulator"
MQTT_STATUS_TOPIC = "manufacturing/status/simulator"


RAW_FILES = {
    "Sensor MQTT CSV": RAW_DIR / "raw_sensor_data.csv",
    "Production CSV": RAW_DIR / "raw_production_data.csv",
    "Quality CSV": RAW_DIR / "raw_quality_data.csv",
}

RAW_TABLES = ["raw_sensor_data", "raw_production_data", "raw_quality_data"]
MART_TABLES = ["dm_daily_production", "dm_daily_quality", "dm_machine_health"]

FLOW_STEPS = [
    ("Virtual Machines", "machine-simulator", "Virtual machines generate realistic machine telemetry."),
    ("MQTT Broker", "mqtt", "Mosquitto exchanges telemetry and control messages."),
    ("Sensor Collector", "collect_sensor_data_from_mqtt", "Airflow collects MQTT telemetry into raw sensor CSV."),
    ("Raw Generator", "generate_raw_data", "Production and quality CSV files are generated."),
    ("Raw Validator", "validate_raw_data", "Required columns, nulls, and duplicate rows are checked."),
    ("Spark ETL", "run_spark_etl", "Spark aggregates production, quality, and machine health data."),
    ("PostgreSQL Raw", "load_to_postgresql", "Raw CSV files are loaded into PostgreSQL."),
    ("Data Mart", "create_data_mart", "BI-ready data mart tables are created."),
    ("Quality Check", "check_data_quality", "Danger machine days are checked."),
]


st.set_page_config(page_title="Manufacturing Flow Viewer", layout="wide")


def mqtt_host() -> str:
    return os.getenv("MQTT_HOST", "localhost")


def mqtt_port() -> int:
    return int(os.getenv("MQTT_PORT", "1883"))


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "manufacturing_dw"),
        user=os.getenv("POSTGRES_USER", "manufacturing"),
        password=os.getenv("POSTGRES_PASSWORD", "manufacturing"),
    )


def mqtt_is_reachable() -> bool:
    try:
        with socket.create_connection((mqtt_host(), mqtt_port()), timeout=1):
            return True
    except OSError:
        return False


def publish_simulator_command(command: str) -> None:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"flow-viewer-control-{int(time.time())}")
    client.connect(mqtt_host(), mqtt_port(), keepalive=30)
    client.loop_start()
    client.publish(MQTT_CONTROL_TOPIC, command.upper(), qos=1, retain=True)
    time.sleep(0.2)
    client.loop_stop()
    client.disconnect()


def get_simulator_status() -> str:
    status = {"value": "UNKNOWN"}

    def on_connect(client, userdata, flags, reason_code, properties):
        client.subscribe(MQTT_STATUS_TOPIC, qos=1)

    def on_message(client, userdata, message):
        try:
            payload = json.loads(message.payload.decode("utf-8"))
            status["value"] = payload.get("status", "UNKNOWN")
        except json.JSONDecodeError:
            status["value"] = message.payload.decode("utf-8", errors="ignore") or "UNKNOWN"

    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"flow-viewer-status-{int(time.time())}")
        client.on_connect = on_connect
        client.on_message = on_message
        client.connect(mqtt_host(), mqtt_port(), keepalive=30)
        client.loop_start()
        time.sleep(0.6)
        client.loop_stop()
        client.disconnect()
    except OSError:
        return "BROKER_OFFLINE"

    return status["value"]


@st.cache_data(ttl=10)
def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


@st.cache_data(ttl=10)
def query_table(table_name: str, limit: int = 500) -> pd.DataFrame:
    try:
        with get_connection() as conn:
            return pd.read_sql_query(f"select * from {table_name} limit {limit}", conn)
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


def step_status() -> dict[str, tuple[str, str]]:
    raw_file_ready = all(path.exists() and file_row_count(path) > 0 for path in RAW_FILES.values())
    raw_table_ready = all(table_count(table) > 0 for table in RAW_TABLES)
    mart_ready = all(table_count(table) > 0 for table in MART_TABLES)
    simulator_status = get_simulator_status()
    broker_ready = mqtt_is_reachable()

    return {
        "machine-simulator": (
            "OK" if simulator_status == "RUNNING" else "WAIT",
            f"status: {simulator_status}",
        ),
        "mqtt": (
            "OK" if broker_ready else "WAIT",
            "broker reachable" if broker_ready else "broker is not reachable",
        ),
        "collect_sensor_data_from_mqtt": (
            "OK" if file_row_count(RAW_FILES["Sensor MQTT CSV"]) > 0 else "WAIT",
            f"{file_row_count(RAW_FILES['Sensor MQTT CSV']):,} rows",
        ),
        "generate_raw_data": (
            "OK" if file_row_count(RAW_FILES["Production CSV"]) > 0 and file_row_count(RAW_FILES["Quality CSV"]) > 0 else "WAIT",
            f"production {file_row_count(RAW_FILES['Production CSV']):,}, quality {file_row_count(RAW_FILES['Quality CSV']):,}",
        ),
        "validate_raw_data": (
            "OK" if raw_file_ready else "WAIT",
            "raw CSV files are ready" if raw_file_ready else "raw CSV files are not ready",
        ),
        "run_spark_etl": (
            "OK" if (DATA_DIR / "processed").exists() else "WAIT",
            "processed directory exists",
        ),
        "load_to_postgresql": (
            "OK" if raw_table_ready else "WAIT",
            "raw tables loaded" if raw_table_ready else "raw tables waiting",
        ),
        "create_data_mart": (
            "OK" if mart_ready else "WAIT",
            "mart tables ready" if mart_ready else "mart tables waiting",
        ),
        "check_data_quality": (
            "OK" if table_count("dm_machine_health") > 0 else "WAIT",
            "machine health mart checked",
        ),
    }


def status_badge(status: str) -> str:
    colors = {"OK": "#0f766e", "WAIT": "#b45309", "FAIL": "#b91c1c"}
    return f"<span style='background:{colors.get(status, '#475569')}; color:white; padding:3px 9px; border-radius:999px; font-size:12px;'>{status}</span>"


def render_simulator_control() -> None:
    status = get_simulator_status()
    col1, col2, col3 = st.columns([1.2, 1, 1])
    col1.metric("Machine Simulator", status)

    if col2.button("Start Publishing", use_container_width=True):
        publish_simulator_command("START")
        st.cache_data.clear()
        st.rerun()

    if col3.button("Stop Publishing", use_container_width=True):
        publish_simulator_command("STOP")
        st.cache_data.clear()
        st.rerun()

    st.caption(f"Control topic: {MQTT_CONTROL_TOPIC}")


def render_flow_cards() -> None:
    statuses = step_status()
    columns = st.columns(3)
    for idx, (title, key, description) in enumerate(FLOW_STEPS):
        status, detail = statuses[key]
        with columns[idx % 3]:
            st.markdown(
                f"""
                <div style="border:1px solid #d1d5db; border-radius:8px; padding:14px 14px 12px; min-height:132px; background:#ffffff;">
                  <div style="display:flex; justify-content:space-between; gap:8px; align-items:center;">
                    <strong style="font-size:15px;">{idx + 1}. {title}</strong>
                    {status_badge(status)}
                  </div>
                  <div style="color:#4b5563; font-size:13px; margin-top:8px;">{description}</div>
                  <div style="color:#111827; font-size:13px; margin-top:10px;">{detail}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_graph() -> None:
    st.graphviz_chart(
        """
        digraph {
          rankdir=LR;
          node [shape=box, style="rounded,filled", fillcolor="#eef2ff", color="#94a3b8", fontname="Arial"];
          control [label="UI On/Off Control", fillcolor="#dcfce7"];
          vm [label="Virtual Machines"];
          mqtt [label="MQTT Broker"];
          collector [label="Sensor Collector"];
          raw [label="Raw CSV"];
          airflow [label="Airflow DAG"];
          spark [label="Spark ETL"];
          pgraw [label="PostgreSQL Raw"];
          mart [label="Data Mart"];
          bi [label="Flow Viewer / BI"];
          control -> mqtt -> vm;
          vm -> mqtt -> collector -> raw -> airflow -> spark -> pgraw -> mart -> bi;
        }
        """,
        use_container_width=True,
    )


def render_kpis() -> None:
    total_production = query_table("dm_daily_production")
    daily_quality = query_table("dm_daily_quality")
    machine_health = query_table("dm_machine_health")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Raw Sensor Rows", f"{table_count('raw_sensor_data'):,}")
    col2.metric("Total Quantity", f"{int(total_production['total_quantity'].sum()) if not total_production.empty else 0:,}")
    col3.metric("Avg Defect Rate", f"{daily_quality['defect_rate'].mean() * 100:.2f}%" if not daily_quality.empty else "0.00%")
    col4.metric("Danger Machines", f"{int((machine_health.get('health_status', pd.Series(dtype=str)) == 'DANGER').sum()) if not machine_health.empty else 0:,}")


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


def render_mart_tab() -> None:
    render_kpis()
    production = query_table("dm_daily_production")
    quality = query_table("dm_daily_quality")
    health = query_table("dm_machine_health")

    left, right = st.columns(2)
    with left:
        st.subheader("Production by Machine")
        if not production.empty:
            fig = px.bar(production, x="machine_id", y="total_quantity", color="product_id", barmode="group")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Data mart is not ready yet.")

    with right:
        st.subheader("Defect Rate by Machine")
        if not quality.empty:
            fig = px.bar(quality, x="machine_id", y="defect_rate", color="machine_id")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Data mart is not ready yet.")

    st.subheader("Machine Health")
    if not health.empty:
        fig = px.scatter(
            health,
            x="avg_temperature",
            y="avg_vibration",
            color="health_status",
            size="avg_pressure",
            hover_data=["machine_id", "event_date"],
        )
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(health, use_container_width=True)
    else:
        st.info("Machine health mart is not ready yet.")


def render_telemetry_tab() -> None:
    sensor = read_csv(RAW_FILES["Sensor MQTT CSV"])
    st.subheader("Latest MQTT Telemetry")
    if sensor.empty:
        st.info("No MQTT sensor data has been collected yet. Start the simulator and run the Airflow DAG.")
        return

    st.dataframe(sensor.tail(100), use_container_width=True)

    if {"event_time", "machine_id", "temperature", "pressure", "vibration"}.issubset(sensor.columns):
        sensor["event_time"] = pd.to_datetime(sensor["event_time"], errors="coerce")
        machine = st.selectbox("Machine", sorted(sensor["machine_id"].dropna().unique()))
        filtered = sensor[sensor["machine_id"] == machine].sort_values("event_time")

        metric = st.radio("Sensor Metric", ["temperature", "pressure", "vibration"], horizontal=True)
        fig = px.line(filtered, x="event_time", y=metric, color="machine_id")
        st.plotly_chart(fig, use_container_width=True)


st.title("Manufacturing Data Pipeline Flow Viewer")
st.caption("Monitor virtual machines, MQTT telemetry, Airflow pipeline outputs, and data mart KPIs.")

overview, telemetry, raw, mart = st.tabs(["Flow", "Telemetry", "Raw Layer", "Data Mart"])

with overview:
    render_simulator_control()
    render_graph()
    render_flow_cards()

with telemetry:
    render_telemetry_tab()

with raw:
    render_raw_tab()

with mart:
    render_mart_tab()


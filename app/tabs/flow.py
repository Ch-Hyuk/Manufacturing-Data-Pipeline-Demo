from __future__ import annotations

import streamlit as st
from requests import RequestException

from shared.config import DATA_DIR, MART_TABLES, MQTT_CONTROL_TOPIC, RAW_FILES
from shared.airflow_api import get_pipeline_snapshot, trigger_pipeline
from shared.data_access import file_row_count, raw_files_ready, raw_tables_ready, table_count
from shared.mqtt_control import get_simulator_status, mqtt_is_reachable, publish_simulator_command


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


def status_badge(status: str) -> str:
    colors = {"OK": "#0f766e", "WAIT": "#b45309", "FAIL": "#b91c1c"}
    return f"<span style='background:{colors.get(status, '#475569')}; color:white; padding:3px 9px; border-radius:999px; font-size:12px;'>{status}</span>"


def step_status() -> dict[str, tuple[str, str]]:
    mart_ready = all(table_count(table) > 0 for table in MART_TABLES)
    simulator_status = get_simulator_status()
    broker_ready = mqtt_is_reachable()

    return {
        "machine-simulator": ("OK" if simulator_status == "RUNNING" else "WAIT", f"status: {simulator_status}"),
        "mqtt": ("OK" if broker_ready else "WAIT", "broker reachable" if broker_ready else "broker is not reachable"),
        "collect_sensor_data_from_mqtt": (
            "OK" if file_row_count(RAW_FILES["Sensor MQTT CSV"]) > 0 else "WAIT",
            f"{file_row_count(RAW_FILES['Sensor MQTT CSV']):,} rows",
        ),
        "generate_raw_data": (
            "OK" if file_row_count(RAW_FILES["Production CSV"]) > 0 and file_row_count(RAW_FILES["Quality CSV"]) > 0 else "WAIT",
            f"production {file_row_count(RAW_FILES['Production CSV']):,}, quality {file_row_count(RAW_FILES['Quality CSV']):,}",
        ),
        "validate_raw_data": (
            "OK" if raw_files_ready() else "WAIT",
            "raw CSV files are ready" if raw_files_ready() else "raw CSV files are not ready",
        ),
        "run_spark_etl": ("OK" if (DATA_DIR / "processed").exists() else "WAIT", "processed directory exists"),
        "load_to_postgresql": ("OK" if raw_tables_ready() else "WAIT", "raw tables loaded" if raw_tables_ready() else "raw tables waiting"),
        "create_data_mart": ("OK" if mart_ready else "WAIT", "mart tables ready" if mart_ready else "mart tables waiting"),
        "check_data_quality": ("OK" if table_count("dm_machine_health") > 0 else "WAIT", "machine health mart checked"),
    }


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


def render_pipeline_control() -> None:
    with st.container(border=True):
        st.subheader("Pipeline Control")
        left, right = st.columns([1, 2])

        if left.button("Run Pipeline", use_container_width=True):
            try:
                dag_run = trigger_pipeline()
                st.success(f"Triggered DAG run: {dag_run.get('dag_run_id')}")
                st.cache_data.clear()
            except RequestException as exc:
                st.error(f"Failed to trigger Airflow DAG: {exc}")

        try:
            snapshot = get_pipeline_snapshot()
        except RequestException as exc:
            right.warning(f"Airflow API is not available yet: {exc}")
            return

        dag_run = snapshot["dag_run"]
        task_instances = snapshot["task_instances"]

        if not dag_run:
            right.info("No DAG run has been created yet.")
            return

        status_col1, status_col2, status_col3 = right.columns(3)
        status_col1.metric("Latest Run", dag_run.get("dag_run_id", "-"))
        status_col2.metric("State", dag_run.get("state", "-"))
        status_col3.metric("Run Type", dag_run.get("run_type", "-"))

        if task_instances:
            rows = [
                {
                    "task_id": task.get("task_id"),
                    "state": task.get("state"),
                    "try_number": task.get("try_number"),
                    "start_date": task.get("start_date"),
                    "end_date": task.get("end_date"),
                }
                for task in task_instances
            ]
            st.dataframe(rows, use_container_width=True, hide_index=True)


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


def render_flow_tab() -> None:
    render_simulator_control()
    render_pipeline_control()
    render_graph()
    render_flow_cards()

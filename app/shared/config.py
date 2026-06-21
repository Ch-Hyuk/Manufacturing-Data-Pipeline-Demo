from __future__ import annotations

import os
from pathlib import Path


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
AIRFLOW_DAG_ID = "manufacturing_data_pipeline"

NUMERIC_COLUMNS = {
    "temperature",
    "pressure",
    "vibration",
    "motor_current",
    "rpm",
    "sequence",
    "quantity",
    "planned_quantity",
    "cycle_time_sec",
    "sample_size",
    "defect_count",
    "total_quantity",
    "total_lot_count",
    "defect_lot_count",
    "defect_rate",
    "avg_temperature",
    "avg_pressure",
    "avg_vibration",
}


def mqtt_host() -> str:
    return os.getenv("MQTT_HOST", "localhost")


def mqtt_port() -> int:
    return int(os.getenv("MQTT_PORT", "1883"))


def airflow_api_base_url() -> str:
    return os.getenv("AIRFLOW_API_BASE_URL", "http://localhost:8080/api/v1")


def airflow_username() -> str:
    return os.getenv("AIRFLOW_USERNAME", "airflow")


def airflow_password() -> str:
    return os.getenv("AIRFLOW_PASSWORD", "airflow")

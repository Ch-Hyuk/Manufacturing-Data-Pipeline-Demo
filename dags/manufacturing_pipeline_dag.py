from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_HOME = Path("/opt/airflow")


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


with DAG(
    dag_id="manufacturing_data_pipeline",
    description="Generate, validate, transform, and load manufacturing data marts.",
    default_args=default_args,
    start_date=datetime(2026, 6, 9),
    schedule="@daily",
    catchup=False,
    tags=["manufacturing", "spark", "postgresql"],
) as dag:
    collect_sensor_data_from_mqtt = BashOperator(
        task_id="collect_sensor_data_from_mqtt",
        bash_command=(
            f"python {PROJECT_HOME}/ingestion/mqtt_sensor_collector.py "
            "--host $MQTT_HOST --port $MQTT_PORT "
            f"--duration-sec 60 --output-file {PROJECT_HOME}/data/raw/raw_sensor_data.csv"
        ),
    )

    generate_raw_data = BashOperator(
        task_id="generate_raw_data",
        bash_command=f"python {PROJECT_HOME}/data_generation/raw_data_generator.py --output-dir {PROJECT_HOME}/data/raw --skip-sensor",
    )

    validate_raw_data = BashOperator(
        task_id="validate_raw_data",
        bash_command=f"python {PROJECT_HOME}/quality/raw_data_validator.py --input-dir {PROJECT_HOME}/data/raw",
    )

    run_spark_etl = BashOperator(
        task_id="run_spark_etl",
        bash_command=(
            f"spark-submit {PROJECT_HOME}/spark/etl_manufacturing.py "
            f"--input-dir {PROJECT_HOME}/data/raw "
            f"--output-dir {PROJECT_HOME}/data/processed"
        ),
    )

    load_to_postgresql = BashOperator(
        task_id="load_to_postgresql",
        bash_command=f"python {PROJECT_HOME}/storage/postgres_loader.py --data-dir {PROJECT_HOME}/data",
    )

    create_data_mart = BashOperator(
        task_id="create_data_mart",
        bash_command=(
            "PGPASSWORD=$POSTGRES_PASSWORD psql "
            "-h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB "
            f"-f {PROJECT_HOME}/sql/02_create_data_marts.sql"
        ),
    )

    check_data_quality = BashOperator(
        task_id="check_data_quality",
        bash_command=(
            "PGPASSWORD=$POSTGRES_PASSWORD psql "
            "-h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USER -d $POSTGRES_DB "
            "-c \"select count(*) as danger_machine_days from dm_machine_health where health_status = 'DANGER';\""
        ),
    )

    notify_result = BashOperator(
        task_id="notify_result",
        bash_command="echo 'Manufacturing data pipeline completed successfully.'",
    )

    (
        collect_sensor_data_from_mqtt
        >> generate_raw_data
        >> validate_raw_data
        >> run_spark_etl
        >> load_to_postgresql
        >> create_data_mart
        >> check_data_quality
        >> notify_result
    )

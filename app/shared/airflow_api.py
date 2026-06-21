from __future__ import annotations

from datetime import datetime, timezone

import requests

from shared.config import AIRFLOW_DAG_ID, airflow_api_base_url, airflow_password, airflow_username


def airflow_auth() -> tuple[str, str]:
    return airflow_username(), airflow_password()


def airflow_url(path: str) -> str:
    return f"{airflow_api_base_url().rstrip('/')}/{path.lstrip('/')}"


def trigger_pipeline() -> dict:
    response = requests.post(
        airflow_url(f"dags/{AIRFLOW_DAG_ID}/dagRuns"),
        auth=airflow_auth(),
        json={
            "dag_run_id": f"manual__flow_viewer__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}",
            "conf": {"triggered_by": "flow_viewer"},
        },
        timeout=10,
    )
    response.raise_for_status()
    return response.json()


def get_latest_dag_run() -> dict:
    response = requests.get(
        airflow_url(f"dags/{AIRFLOW_DAG_ID}/dagRuns"),
        auth=airflow_auth(),
        params={"order_by": "-start_date", "limit": 1},
        timeout=10,
    )
    response.raise_for_status()
    dag_runs = response.json().get("dag_runs", [])
    return dag_runs[0] if dag_runs else {}


def get_task_instances(dag_run_id: str) -> list[dict]:
    if not dag_run_id:
        return []
    response = requests.get(
        airflow_url(f"dags/{AIRFLOW_DAG_ID}/dagRuns/{dag_run_id}/taskInstances"),
        auth=airflow_auth(),
        timeout=10,
    )
    response.raise_for_status()
    return response.json().get("task_instances", [])


def get_pipeline_snapshot() -> dict:
    latest_run = get_latest_dag_run()
    return {
        "dag_run": latest_run,
        "task_instances": get_task_instances(latest_run.get("dag_run_id", "")),
    }


#!/usr/bin/env bash
set -e

airflow db migrate

airflow users create \
  --username airflow \
  --password airflow \
  --firstname Data \
  --lastname Engineer \
  --role Admin \
  --email airflow@example.com || true

airflow scheduler &
exec airflow webserver

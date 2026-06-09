from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


RAW_TABLES = {
    "raw_sensor_data": "data/raw/raw_sensor_data.csv",
    "raw_production_data": "data/raw/raw_production_data.csv",
    "raw_quality_data": "data/raw/raw_quality_data.csv",
}


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "manufacturing_dw"),
        user=os.getenv("POSTGRES_USER", "manufacturing"),
        password=os.getenv("POSTGRES_PASSWORD", "manufacturing"),
    )


def load_csv(conn, table_name: str, csv_path: Path) -> None:
    df = pd.read_csv(csv_path)
    columns = list(df.columns)
    rows = [tuple(None if pd.isna(value) else value for value in row) for row in df.to_numpy()]

    with conn.cursor() as cursor:
        cursor.execute(f"truncate table {table_name};")
        execute_values(
            cursor,
            f"insert into {table_name} ({', '.join(columns)}) values %s",
            rows,
        )
    conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    with get_connection() as conn:
        for table_name, relative_path in RAW_TABLES.items():
            csv_path = data_dir.parent / relative_path if data_dir.name == "data" else data_dir / Path(relative_path).relative_to("data")
            load_csv(conn, table_name, csv_path)
            print(f"Loaded {csv_path} into {table_name}")


if __name__ == "__main__":
    main()

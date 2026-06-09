from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


REQUIRED_COLUMNS = {
    "raw_sensor_data.csv": ["event_time", "machine_id", "temperature", "pressure", "vibration"],
    "raw_production_data.csv": ["production_date", "lot_id", "machine_id", "product_id", "quantity"],
    "raw_quality_data.csv": ["inspection_time", "lot_id", "result", "defect_type"],
}


def validate_file(path: Path, required_columns: list[str]) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")

    df = pd.read_csv(path)
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(f"{path.name} is missing columns: {missing_columns}")

    required_without_nullable = [column for column in required_columns if column != "defect_type"]
    null_counts = df[required_without_nullable].isnull().sum()
    invalid_nulls = null_counts[null_counts > 0]
    if not invalid_nulls.empty:
        raise ValueError(f"{path.name} has nulls: {invalid_nulls.to_dict()}")

    duplicated_rows = df.duplicated().sum()
    if duplicated_rows > 0:
        raise ValueError(f"{path.name} has duplicated rows: {duplicated_rows}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default="data/raw")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    for file_name, columns in REQUIRED_COLUMNS.items():
        validate_file(input_dir / file_name, columns)

    print(f"Validated raw manufacturing data in {input_dir}")


if __name__ == "__main__":
    main()

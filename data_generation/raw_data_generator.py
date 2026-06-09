from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


try:
    from faker import Faker
except ImportError:
    Faker = None


fake = Faker("ko_KR") if Faker else None


MACHINES = [f"M{i:02d}" for i in range(1, 7)]
PRODUCTS = ["P1001", "P1002", "P1003", "P2001"]
DEFECT_TYPES = ["scratch", "size_error", "contamination", "assembly_error"]
FALLBACK_NAMES = ["Kim Minjun", "Lee Seoyeon", "Park Jiho", "Choi Yuna", "Jung Dohyun"]
SHIFTS = ["A", "B", "C"]
PRODUCT_STANDARD_QUANTITY = {"P1001": 120, "P1002": 105, "P1003": 95, "P2001": 80}


def generate_sensor_data(base_time: datetime, rows: int) -> pd.DataFrame:
    records = []
    for i in range(rows):
        machine_id = random.choice(MACHINES)
        records.append(
            {
                "event_time": base_time + timedelta(minutes=i * 5),
                "machine_id": machine_id,
                "temperature": round(random.normalvariate(72, 7), 2),
                "pressure": round(random.normalvariate(4.2, 0.45), 2),
                "vibration": round(max(0.01, random.normalvariate(0.16, 0.06)), 3),
            }
        )
    return pd.DataFrame(records)


def generate_production_data(base_date: datetime, lots: int) -> pd.DataFrame:
    records = []
    for i in range(lots):
        machine_id = random.choice(MACHINES)
        product_id = random.choice(PRODUCTS)
        lot_id = f"LOT{base_date:%Y%m%d}{i + 1:04d}"
        standard_quantity = PRODUCT_STANDARD_QUANTITY[product_id]
        shift = SHIFTS[(i // max(1, lots // len(SHIFTS))) % len(SHIFTS)]
        records.append(
            {
                "production_date": base_date.date(),
                "lot_id": lot_id,
                "machine_id": machine_id,
                "product_id": product_id,
                "shift": shift,
                "planned_quantity": standard_quantity,
                "quantity": max(20, int(random.normalvariate(standard_quantity, standard_quantity * 0.12))),
                "cycle_time_sec": round(random.normalvariate(8.5, 1.1), 2),
                "operator_name": fake.name() if fake else random.choice(FALLBACK_NAMES),
            }
        )
    return pd.DataFrame(records)


def generate_quality_data(production_df: pd.DataFrame, base_time: datetime) -> pd.DataFrame:
    records = []
    for idx, row in production_df.iterrows():
        machine_risk = {"M01": 0.06, "M02": 0.08, "M03": 0.05, "M04": 0.11, "M05": 0.09, "M06": 0.14}
        product_risk = {"P1001": 0.05, "P1002": 0.07, "P1003": 0.1, "P2001": 0.12}
        shift_risk = {"A": 0.0, "B": 0.015, "C": 0.035}
        fail_probability = machine_risk[row["machine_id"]] + product_risk[row["product_id"]] + shift_risk[row["shift"]]
        is_fail = random.random() < fail_probability
        records.append(
            {
                "inspection_time": base_time + timedelta(minutes=idx * 9),
                "lot_id": row["lot_id"],
                "result": "FAIL" if is_fail else "PASS",
                "defect_type": random.choice(DEFECT_TYPES) if is_fail else None,
                "sample_size": random.randint(5, 12),
                "defect_count": random.randint(1, 4) if is_fail else 0,
            }
        )
    return pd.DataFrame(records)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="data/raw")
    parser.add_argument("--sensor-rows", type=int, default=1000)
    parser.add_argument("--lots", type=int, default=180)
    parser.add_argument("--skip-sensor", action="store_true")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    base_time = datetime.now().replace(hour=8, minute=0, second=0, microsecond=0)
    production_df = generate_production_data(base_time, args.lots)

    if not args.skip_sensor:
        generate_sensor_data(base_time, args.sensor_rows).to_csv(output_dir / "raw_sensor_data.csv", index=False)
    production_df.to_csv(output_dir / "raw_production_data.csv", index=False)
    generate_quality_data(production_df, base_time).to_csv(output_dir / "raw_quality_data.csv", index=False)

    print(f"Generated raw manufacturing data in {output_dir}")


if __name__ == "__main__":
    main()

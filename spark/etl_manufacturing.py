from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("manufacturing-data-pipeline")
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .getOrCreate()
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default="data/raw")
    parser.add_argument("--output-dir", default="data/processed")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)

    spark = build_spark()

    sensor_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(input_dir / "raw_sensor_data.csv"))
        .withColumn("event_date", F.to_date("event_time"))
    )
    production_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(input_dir / "raw_production_data.csv"))
    )
    quality_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(str(input_dir / "raw_quality_data.csv"))
    )

    daily_production = production_df.groupBy("production_date", "machine_id", "product_id").agg(
        F.sum("quantity").alias("total_quantity")
    )

    quality_joined = quality_df.join(production_df.select("lot_id", "production_date", "machine_id"), "lot_id", "left")
    daily_quality = quality_joined.groupBy("production_date", "machine_id").agg(
        F.count("lot_id").alias("total_lot_count"),
        F.sum(F.when(F.col("result") == "FAIL", 1).otherwise(0)).alias("defect_lot_count"),
    ).withColumn(
        "defect_rate",
        F.round(F.col("defect_lot_count") / F.col("total_lot_count"), 4),
    )

    machine_health = sensor_df.groupBy("event_date", "machine_id").agg(
        F.round(F.avg("temperature"), 2).alias("avg_temperature"),
        F.round(F.avg("pressure"), 2).alias("avg_pressure"),
        F.round(F.avg("vibration"), 3).alias("avg_vibration"),
    ).withColumn(
        "health_status",
        F.when((F.col("avg_temperature") >= 82) | (F.col("avg_pressure") >= 5.0) | (F.col("avg_vibration") >= 0.25), "DANGER")
        .when((F.col("avg_temperature") >= 78) | (F.col("avg_pressure") >= 4.7) | (F.col("avg_vibration") >= 0.21), "WARNING")
        .otherwise("NORMAL"),
    )

    daily_production.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output_dir / "dm_daily_production"))
    daily_quality.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output_dir / "dm_daily_quality"))
    machine_health.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output_dir / "dm_machine_health"))

    spark.stop()


if __name__ == "__main__":
    main()


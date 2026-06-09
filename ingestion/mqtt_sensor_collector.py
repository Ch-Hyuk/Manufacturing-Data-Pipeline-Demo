from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path

import paho.mqtt.client as mqtt


FIELDNAMES = [
    "event_time",
    "factory_id",
    "line_id",
    "machine_id",
    "product_id",
    "mode",
    "temperature",
    "pressure",
    "vibration",
    "motor_current",
    "rpm",
    "anomaly_type",
    "sequence",
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--topic", default="manufacturing/+/+/+/telemetry")
    parser.add_argument("--duration-sec", type=int, default=60)
    parser.add_argument("--output-file", default="data/raw/raw_sensor_data.csv")
    args = parser.parse_args()

    output_file = Path(args.output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    records: list[dict] = []

    def on_connect(client, userdata, flags, reason_code, properties):
        client.subscribe(args.topic, qos=1)

    def on_message(client, userdata, message):
        try:
            payload = json.loads(message.payload.decode("utf-8"))
        except json.JSONDecodeError:
            return
        records.append({field: payload.get(field) for field in FIELDNAMES})

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="airflow-sensor-collector")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(args.host, args.port, keepalive=60)
    client.loop_start()

    deadline = time.time() + args.duration_sec
    while time.time() < deadline:
        time.sleep(0.5)

    client.loop_stop()
    client.disconnect()

    if not records:
        raise RuntimeError("No MQTT telemetry messages were collected.")

    with output_file.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(records)

    print(f"Collected {len(records)} MQTT telemetry rows into {output_file}")


if __name__ == "__main__":
    main()

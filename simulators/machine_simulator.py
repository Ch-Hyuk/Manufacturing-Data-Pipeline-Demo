from __future__ import annotations

import argparse
import json
import math
import random
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import paho.mqtt.client as mqtt


CONTROL_TOPIC = "manufacturing/control/simulator"
STATUS_TOPIC = "manufacturing/status/simulator"


@dataclass
class MachineState:
    machine_id: str
    line_id: str
    product_id: str
    base_temperature: float
    base_pressure: float
    base_vibration: float
    cycle_time_sec: float
    drift: float = 0.0
    mode: str = "RUN"
    sequence: int = 0
    anomaly_until: float = 0.0
    anomaly_type: str | None = None
    last_maintenance_ts: float = field(default_factory=time.time)

    def maybe_change_state(self, now: float) -> None:
        if random.random() < 0.004:
            self.mode = random.choices(["RUN", "IDLE", "SETUP"], weights=[0.82, 0.12, 0.06])[0]

        hours_since_maintenance = (now - self.last_maintenance_ts) / 3600
        self.drift = min(4.5, hours_since_maintenance * 0.08)

        if now > self.anomaly_until and random.random() < 0.01:
            self.anomaly_type = random.choice(["bearing_wear", "cooling_issue", "pressure_leak"])
            self.anomaly_until = now + random.randint(45, 120)

        if now > self.anomaly_until:
            self.anomaly_type = None

    def read_sensor(self, now: float) -> dict:
        self.sequence += 1
        self.maybe_change_state(now)

        cycle_wave = math.sin(self.sequence / self.cycle_time_sec)
        load_factor = {"RUN": 1.0, "SETUP": 0.65, "IDLE": 0.25}[self.mode]

        temperature = self.base_temperature + self.drift + cycle_wave * 1.6 + random.normalvariate(0, 0.45)
        pressure = self.base_pressure + cycle_wave * 0.12 + random.normalvariate(0, 0.04)
        vibration = self.base_vibration + abs(cycle_wave) * 0.025 + random.normalvariate(0, 0.008)

        if self.anomaly_type == "bearing_wear":
            vibration += random.uniform(0.08, 0.16)
            temperature += random.uniform(1.5, 3.5)
        elif self.anomaly_type == "cooling_issue":
            temperature += random.uniform(6.0, 11.0)
        elif self.anomaly_type == "pressure_leak":
            pressure -= random.uniform(0.45, 0.85)
            vibration += random.uniform(0.02, 0.05)

        return {
            "event_time": datetime.now(timezone.utc).isoformat(),
            "factory_id": "F01",
            "line_id": self.line_id,
            "machine_id": self.machine_id,
            "product_id": self.product_id,
            "mode": self.mode,
            "temperature": round(temperature * load_factor + self.base_temperature * (1 - load_factor), 2),
            "pressure": round(max(0.1, pressure * load_factor), 2),
            "vibration": round(max(0.001, vibration * load_factor), 3),
            "motor_current": round(random.normalvariate(18.0, 1.4) * load_factor, 2),
            "rpm": int(random.normalvariate(1450, 35) * load_factor),
            "anomaly_type": self.anomaly_type,
            "sequence": self.sequence,
        }


def build_machines() -> list[MachineState]:
    return [
        MachineState("M01", "L01", "P1001", 70.0, 4.25, 0.12, 9.5),
        MachineState("M02", "L01", "P1002", 72.0, 4.35, 0.14, 10.0),
        MachineState("M03", "L01", "P1003", 69.5, 4.10, 0.11, 11.0),
        MachineState("M04", "L02", "P1001", 73.5, 4.45, 0.15, 8.5),
        MachineState("M05", "L02", "P2001", 71.0, 4.15, 0.13, 9.0),
        MachineState("M06", "L02", "P2001", 74.0, 4.55, 0.16, 8.0),
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--interval-sec", type=float, default=1.0)
    args = parser.parse_args()

    simulator_enabled = threading.Event()
    simulator_enabled.set()

    def publish_status(status: str) -> None:
        client.publish(
            STATUS_TOPIC,
            json.dumps({"status": status, "event_time": datetime.now(timezone.utc).isoformat()}),
            qos=1,
            retain=True,
        )

    def on_connect(client, userdata, flags, reason_code, properties):
        client.subscribe(CONTROL_TOPIC, qos=1)
        publish_status("RUNNING" if simulator_enabled.is_set() else "STOPPED")

    def on_message(client, userdata, message):
        command = message.payload.decode("utf-8").strip().upper()
        if command in {"START", "ON", "RUN"}:
            simulator_enabled.set()
            publish_status("RUNNING")
            print("Machine simulator publishing enabled.")
        elif command in {"STOP", "OFF", "PAUSE"}:
            simulator_enabled.clear()
            publish_status("STOPPED")
            print("Machine simulator publishing paused.")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"machine-simulator-{random.randint(1000, 9999)}")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(args.host, args.port, keepalive=60)
    client.loop_start()

    machines = build_machines()
    print(f"Publishing machine telemetry to mqtt://{args.host}:{args.port}")

    try:
        while True:
            if simulator_enabled.is_set():
                now = time.time()
                for machine in machines:
                    payload = machine.read_sensor(now)
                    topic = f"manufacturing/F01/{machine.line_id}/{machine.machine_id}/telemetry"
                    client.publish(topic, json.dumps(payload), qos=1)
            time.sleep(args.interval_sec)
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()

from __future__ import annotations

import json
import socket
import time

import paho.mqtt.client as mqtt

from shared.config import MQTT_CONTROL_TOPIC, MQTT_STATUS_TOPIC, mqtt_host, mqtt_port


def mqtt_is_reachable() -> bool:
    try:
        with socket.create_connection((mqtt_host(), mqtt_port()), timeout=1):
            return True
    except OSError:
        return False


def publish_simulator_command(command: str) -> None:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"flow-viewer-control-{int(time.time())}")
    client.connect(mqtt_host(), mqtt_port(), keepalive=30)
    client.loop_start()
    client.publish(MQTT_CONTROL_TOPIC, command.upper(), qos=1, retain=True)
    time.sleep(0.2)
    client.loop_stop()
    client.disconnect()


def get_simulator_status() -> str:
    status = {"value": "UNKNOWN"}

    def on_connect(client, userdata, flags, reason_code, properties):
        client.subscribe(MQTT_STATUS_TOPIC, qos=1)

    def on_message(client, userdata, message):
        try:
            payload = json.loads(message.payload.decode("utf-8"))
            status["value"] = payload.get("status", "UNKNOWN")
        except json.JSONDecodeError:
            status["value"] = message.payload.decode("utf-8", errors="ignore") or "UNKNOWN"

    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"flow-viewer-status-{int(time.time())}")
        client.on_connect = on_connect
        client.on_message = on_message
        client.connect(mqtt_host(), mqtt_port(), keepalive=30)
        client.loop_start()
        time.sleep(0.6)
        client.loop_stop()
        client.disconnect()
    except OSError:
        return "BROKER_OFFLINE"

    return status["value"]


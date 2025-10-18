import json
import random
import threading
import time
from time import sleep

from paho.mqtt import client as mqttc
from paho.mqtt.client import CallbackAPIVersion, MQTTMessageInfo

HOST = "localhost"
PORT = 1883
PPG_PREFIX = "wearables/ppg_sensor/"
ECG_PREFIX = "wearables/ecg_sensor/"

# Number of Sensors that will be simulated
PPGSENSORS = 5
ECGSENSORS = 5
SEND_INTERVAL = 2 #sek

stop_event = threading.Event()


def simulating_sensor(sensor_type, client_id):
    device_id = f"{sensor_type}_sensor_{client_id}"
    mqtt_client = mqttc.Client(CallbackAPIVersion.VERSION2, device_id)
    mqtt_client.connect(HOST, PORT)
    mqtt_client.loop_start()

    try:
        while not stop_event.is_set():
            if sensor_type == "ppg":
                publish_ppg_message(mqtt_client, device_id, client_id)
            else:
                publish_ecg_message(mqtt_client, device_id, client_id)

        sleep(SEND_INTERVAL * random.uniform(0, 1))
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        print(f"Device {device_id} stopped")

def publish_ppg_message(mqtt_client, device_id, client_id) -> MQTTMessageInfo:
    ir_value = random.randint(40000, 60000)
    red_value = random.randint(40000, 60000)
    green_value = random.randint(40000, 60000)
    heart_rate_bpm = random.randint(70, 80)
    spo2_percent = round(random.uniform(97, 100))

    topic = f"{PPG_PREFIX}{device_id}"

    mock_ppg = {
        "device_id": device_id,
        "user_id": client_id,
        "data": [
            {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "ir_value": ir_value,
                "red_value": red_value,
                "green_value": green_value,
                "heart_rate_bpm": heart_rate_bpm,
                "spo2_percent": spo2_percent
            }
        ]
    }

    return mqtt_client.publish(topic, json.dumps(mock_ppg))


def publish_ecg_message(mqtt_client, device_id, client_id) -> MQTTMessageInfo:
    leads = []

    for i in range(1, 6):
        leads.append(round(random.uniform(-0.2,0.2), 2))

    topic = f"{ECG_PREFIX}{device_id}"
    heart_rate_bpm = random.randint(70, 100)
    mock_ecg = {
        "device_id": device_id,
        "client_id": client_id,
        "data": [
            {
                "lead-0": leads[0],
                "lead-1": leads[1],
                "lead-2": leads[2],
                "lead-3": leads[3],
                "lead-4": leads[4],
                "heart_rate_bpm": heart_rate_bpm
            }
        ]
    }

    return mqtt_client.publish(topic, json.dumps(mock_ecg))

def main():
    ppgs = []
    ecgs = []

    for i in range(1, PPGSENSORS + 1):
        ppg_sensor = threading.Thread(target=simulating_sensor, args=("ppg", i))
        ppg_sensor.start()
        ppgs.append(ppg_sensor)

    for i in range(1, ECGSENSORS + 1):
        ecg_sensor = threading.Thread(target=simulating_sensor, args=("ecg", i))
        ecg_sensor.start()
        ecgs.append(ecg_sensor)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping all sensors...")
        stop_event.set()

    for t in ppgs + ecgs:
        t.join()

    print("All Threads stopped")

if __name__ == "__main__":
    main()
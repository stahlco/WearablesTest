import json
import threading

import paho.mqtt.client as mqtt
import os
from dotenv import load_dotenv
import random
import time

load_dotenv()

BROKER = os.getenv("BROKER_ADDR")
PORT = os.getenv("BROKER_PORT") # standard mosquitto port
TOPIC_PREFIX = os.getenv("TOPIC_PREFIX")

SENSORS = os.getenv("SENSOR_COUNT")
INTERVAL = os.getenv("MEASURE_INTERVAL")


def simulate_sensor(sensor_id):
    mqtt_client = mqtt.Client(f"sensor_{sensor_id}")
    mqtt_client.connect(BROKER, int(PORT))
    mqtt_client.loop_start()

    topic = f"{TOPIC_PREFIX}sensor_{sensor_id}"

    try:
        while True:
            temperature = round(random.gauss(20,25))

            pl = {
                "sensor_id": sensor_id,
                "temperature": temperature,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }

            mqtt_client.publish(topic, json.dumps(pl))

            time.sleep(int(INTERVAL))
    except KeyboardInterrupt:
        print(f"Sensor {sensor_id} stopped.")
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()


def main():
    threads = []

    for i in range(1, int(SENSORS) + 1):
        t = threading.Thread(target=simulate_sensor, args=(i,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
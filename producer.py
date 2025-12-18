
import json
import os
import random
import time
from datetime import datetime

from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

# Load environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
SASL_USERNAME = os.getenv("SASL_USERNAME")
SASL_PASSWORD = os.getenv("SASL_PASSWORD")
SECURITY_PROTOCOL = os.getenv("SECURITY_PROTOCOL", "SASL_SSL")
SASL_MECHANISM = os.getenv("SASL_MECHANISM", "PLAIN")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "vitals_topic")
GENERATION_INTERVAL_MS = int(os.getenv("GENERATION_INTERVAL_MS", "1000"))


def generate_vitals_data(patient_id):
    now = datetime.utcnow().isoformat() + "Z"
    body_temperature = round(random.uniform(36.0, 40.0), 1)  # Celsius
    heart_rate = random.randint(60, 120)  # BPM
    systolic_blood_pressure = random.randint(90, 160)  # mmHg
    diastolic_blood_pressure = random.randint(60, 100)  # mmHg
    respiratory_rate = random.randint(12, 20)  # Breaths per minute
    oxygen_saturation = random.randint(95, 100)  # %
    blood_glucose = random.randint(70, 140)  # mg/dL

    return {
        "patient_id": patient_id,
        "timestamp": now,
        "body_temperature": body_temperature,
        "heart_rate": heart_rate,
        "systolic_blood_pressure": systolic_blood_pressure,
        "diastolic_blood_pressure": diastolic_blood_pressure,
        "respiratory_rate": respiratory_rate,
        "oxygen_saturation": oxygen_saturation,
        "blood_glucose": blood_glucose,
    }


def create_kafka_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        sasl_plain_username=SASL_USERNAME,
        sasl_plain_password=SASL_PASSWORD,
        security_protocol=SECURITY_PROTOCOL,
        sasl_mechanism=SASL_MECHANISM,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    return producer


def main():
    producer = create_kafka_producer()
    patient_ids = [f"patient_{i}" for i in range(1, 6)]  # Simulate 5 patients

    while True:
        for patient_id in patient_ids:
            vitals_data = generate_vitals_data(patient_id)
            print(f"Sending data: {vitals_data}")
            producer.send(OUTPUT_TOPIC, value=vitals_data)

        producer.flush()  # Ensure all messages are sent
        time.sleep(GENERATION_INTERVAL_MS / 1000)  # Convert milliseconds to seconds

if __name__ == "__main__":
    main()

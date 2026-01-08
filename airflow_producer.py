import json
import time
import random
import uuid
import signal
import sys
from confluent_kafka import Producer

INVALID_EVENT_PROBABILITY = 0.3
TOPIC = "userEvents"

producer = Producer({
    "bootstrap.servers": "localhost:9094",
    "linger.ms": 10,
    "retries": 3,
    "acks": "all"
})

EVENT_TYPES = ["login", "purchase", "logout"]
OS_TYPES = ["android", "ios", "web"]
COUNTRIES = ["PK", "IN", "US"]
CITIES = ["Lahore", "Karachi", "Delhi", "NYC"]


def generate_valid_event():
    return {
        "schema_version": "v1",
        "payload": {
            "event_id": str(uuid.uuid4()),
            "user_id": f"user_{random.randint(1, 1000)}",
            "event_type": random.choice(EVENT_TYPES),
            "timestamp": int(time.time() * 1000),
            "device": {
                "os": random.choice(OS_TYPES),
                "app_version": f"1.{random.randint(0,5)}.{random.randint(0,9)}",
            },
            "geo": {
                "country": random.choice(COUNTRIES),
                "city": random.choice(CITIES)
            },
            "amount": round(random.uniform(10, 500), 2)
        },
    }


def generate_invalid_event():
    return {
        "schema_version": "v2",
        "payload": {
            "event_id": str(uuid.uuid4()),
            "user_id": random.randint(1, 1000),  # wrong type
            "event_type": random.choice(EVENT_TYPES),
            "device": {
                "os": random.choice(OS_TYPES)
            }
        },
    }


def generate_event():
    if random.random() < INVALID_EVENT_PROBABILITY:
        return generate_invalid_event(), "INVALID"
    return generate_valid_event(), "VALID"


def shutdown_handler(sig, frame):
    print("\n[SHUTDOWN] Flushing Kafka producer...")
    producer.flush()
    sys.exit(0)


def main():
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    print("[START] Kafka producer started")

    while True:
        event, event_type = generate_event()

        producer.produce(
            topic=TOPIC,
            value=json.dumps(event).encode("utf-8")
        )

        producer.poll(0)  

        print(f"[{event_type} EVENT]", event)
        time.sleep(2)


if __name__ == "__main__":
    main()

from confluent_kafka import Producer
import json
import time
import random
from datetime import datetime,timezone  

# Configure Confluent Kafka producer
conf = {
    'bootstrap.servers': 'localhost:9094',  # or your broker address
    'client.id': 'flight-data-producer'
}
producer = Producer(conf)

# Sample airlines
airlines = ['PIA', 'Qatar Airways', 'Emirates', 'Turkish Airlines']

def generate_flight():
    return {
        "flight": f"PK{random.randint(100, 999)}",
        "airline": random.choice(airlines),
        "altitude": random.randint(30000, 40000),
        "speed": random.randint(500, 600),
        "status": "in_air",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        # "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    }

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed:', err)
    else:
        print(f'Sent to {msg.topic()} partition {msg.partition()} @ offset {msg.offset()}')

if __name__ == '__main__':
    try:
        while True:
            flight_data = generate_flight()
            producer.produce(
                topic='flight',
                key=flight_data["flight"],
                value=json.dumps(flight_data),
                callback=delivery_report
            )
            producer.poll(0)
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()

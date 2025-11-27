from confluent_kafka import Producer
import json
import random
import time
from datetime import datetime

# Confluent Kafka broker
bootstrap_servers = 'localhost:9092'  # Use 'broker:9092' if inside Docker
topic_name = 'transactions'

# Create Producer instance
conf = {
    'bootstrap.servers': bootstrap_servers
}
producer = Producer(conf)

# Dummy customers and locations
customers = ['Alice', 'Bob', 'Charlie', 'David', 'Eva']
locations = ['NY', 'CA', 'TX', 'FL', 'IL']

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Produced: {msg.value().decode('utf-8')} to {msg.topic()} [{msg.partition()}]")

while True:
    customer_id = random.choice(customers)
    amount = round(random.uniform(10.0, 3000.0), 2)
    location = random.choice(locations)

    transaction = {
        'customer_id': customer_id,
        'transaction_id': random.randint(100000, 999999),
        'amount': amount,
        'location': location,
        'timestamp': datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    }

    # Produce message
    producer.produce(
        topic=topic_name,
        value=json.dumps(transaction),
        callback=delivery_report
    )
    producer.poll(0)  # serve delivery reports

    time.sleep(random.uniform(0.1, 1.0))  # simulate random transaction timing

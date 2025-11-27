# from kafka import KafkaConsumer
# import json
# from datetime import datetime, timedelta
# from collections import defaultdict, deque

# bootstrap_servers = 'localhost:9092'
# topic_name = 'transactions'

# consumer = KafkaConsumer(
#     topic_name,
#     bootstrap_servers=bootstrap_servers,
#     value_deserializer=lambda m: json.loads(m.decode('utf-8')),
#     auto_offset_reset='earliest',
#     enable_auto_commit=True
# )

# # --- Fraud detection parameters ---
# MAX_AMOUNT = 1000  # single transaction threshold
# RAPID_COUNT = 3    # number of transactions
# RAPID_SECONDS = 10 # seconds window for rapid transactions
# SUSPICIOUS_LOCATIONS = {'TX', 'FL'}  # example suspicious states
# LOCATION_JUMP_WINDOW = 60  # seconds window to detect location jump

# # State: store last transactions per customer
# customer_history = defaultdict(lambda: deque())  # {customer_id: deque([(timestamp, amount, location), ...])}

# def is_fraud(transaction):
#     alerts = []

#     customer = transaction['customer_id']
#     amount = transaction['amount']
#     location = transaction['location']
#     timestamp = datetime.fromisoformat(transaction['timestamp'])

#     # --- Rule 1: High amount ---
#     if amount > MAX_AMOUNT:
#         alerts.append(f"High amount: ${amount}")

#     # --- Rule 2: Rapid multiple transactions ---
#     history = customer_history[customer]

#     # Add current transaction to history
#     history.append((timestamp, amount, location))
#     # Remove transactions older than max(RAPID_SECONDS, LOCATION_JUMP_WINDOW)
#     while history and (timestamp - history[0][0]).total_seconds() > max(RAPID_SECONDS, LOCATION_JUMP_WINDOW):
#         history.popleft()

#     if len([tx for tx in history if (timestamp - tx[0]).total_seconds() <= RAPID_SECONDS]) >= RAPID_COUNT:
#         alerts.append(f"Rapid transactions: {len([tx for tx in history if (timestamp - tx[0]).total_seconds() <= RAPID_SECONDS])} txs in last {RAPID_SECONDS} sec")

#     # --- Rule 3: Suspicious location ---
#     if location in SUSPICIOUS_LOCATIONS:
#         alerts.append(f"Suspicious location: {location}")

#     # --- Rule 4: Velocity anomaly ---
#     total_recent_amount = sum(tx[1] for tx in history if (timestamp - tx[0]).total_seconds() <= RAPID_SECONDS)
#     if total_recent_amount > 2000:
#         alerts.append(f"Velocity anomaly: total ${total_recent_amount} in last {RAPID_SECONDS} sec")

#     # --- Rule 5: Location jump ---
#     recent_locations = {tx[2] for tx in history if (timestamp - tx[0]).total_seconds() <= LOCATION_JUMP_WINDOW}
#     if len(recent_locations) > 1:
#         alerts.append(f"Location jump detected: transactions in {recent_locations} within {LOCATION_JUMP_WINDOW} sec")

#     return alerts

# print("Starting enhanced fraud detection with location jump...")

# for message in consumer:
#     transaction = message.value
#     alerts = is_fraud(transaction)
    
#     if alerts:
#         print(f"⚠️ Fraud alert for {transaction['customer_id']}: {alerts} | Transaction: {transaction}")
#     else:
#         print(f"✅ Legitimate transaction: {transaction}")

from confluent_kafka import Consumer, KafkaException
import json
from datetime import datetime, timedelta
from collections import defaultdict, deque

bootstrap_servers = 'localhost:9092'  # Use 'broker:9092' if inside Docker
topic_name = 'transactions'
group_id = 'fraud-detector-group'

# Create Consumer instance
conf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe([topic_name])

# --- Fraud detection parameters ---
MAX_AMOUNT = 1000  # single transaction threshold
RAPID_COUNT = 3    # number of transactions
RAPID_SECONDS = 10 # seconds window for rapid transactions
SUSPICIOUS_LOCATIONS = {'TX', 'FL'}  # example suspicious states
LOCATION_JUMP_WINDOW = 60  # seconds window to detect location jump

# State: store last transactions per customer
customer_history = defaultdict(lambda: deque())  # {customer_id: deque([(timestamp, amount, location), ...])}

def is_fraud(transaction):
    alerts = []

    customer = transaction['customer_id']
    amount = transaction['amount']
    location = transaction['location']
    timestamp = datetime.fromisoformat(transaction['timestamp'])

    # --- Rule 1: High amount ---
    if amount > MAX_AMOUNT:
        alerts.append(f"High amount: ${amount}")

    # --- Rule 2: Rapid multiple transactions ---
    history = customer_history[customer]
    history.append((timestamp, amount, location))
    # Remove old transactions
    while history and (timestamp - history[0][0]).total_seconds() > max(RAPID_SECONDS, LOCATION_JUMP_WINDOW):
        history.popleft()

    if len([tx for tx in history if (timestamp - tx[0]).total_seconds() <= RAPID_SECONDS]) >= RAPID_COUNT:
        alerts.append(f"Rapid transactions: {len([tx for tx in history if (timestamp - tx[0]).total_seconds() <= RAPID_SECONDS])} txs in last {RAPID_SECONDS} sec")

    # --- Rule 3: Suspicious location ---
    if location in SUSPICIOUS_LOCATIONS:
        alerts.append(f"Suspicious location: {location}")

    # --- Rule 4: Velocity anomaly ---
    total_recent_amount = sum(tx[1] for tx in history if (timestamp - tx[0]).total_seconds() <= RAPID_SECONDS)
    if total_recent_amount > 2000:
        alerts.append(f"Velocity anomaly: total ${total_recent_amount} in last {RAPID_SECONDS} sec")

    # --- Rule 5: Location jump ---
    recent_locations = {tx[2] for tx in history if (timestamp - tx[0]).total_seconds() <= LOCATION_JUMP_WINDOW}
    if len(recent_locations) > 1:
        alerts.append(f"Location jump detected: transactions in {recent_locations} within {LOCATION_JUMP_WINDOW} sec")

    return alerts

print("Starting enhanced fraud detection with Confluent Kafka...")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        
        transaction = json.loads(msg.value().decode('utf-8'))
        alerts = is_fraud(transaction)
        
        if alerts:
            print(f"⚠️ Fraud alert for {transaction['customer_id']}: {alerts} | Transaction: {transaction}")
        else:
            print(f"✅ Legitimate transaction: {transaction}")

except KeyboardInterrupt:
    print("Shutting down consumer...")

finally:
    consumer.close()

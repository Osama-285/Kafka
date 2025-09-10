import json
from confluent_kafka import Consumer, KafkaError

BOOTSTRAP = "localhost:9094"
TOPIC = "bank.transactions"

# Fraud detection rules
BLACKLIST_MERCHANTS = {"SCAM_SHOP", "FAKE_STORE", "DARK_MARKET"}
IMPOSSIBLE_AMOUNT = 8000  # threshold for suspiciously high amount

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "bank-txn-consumer",
    "auto.offset.reset": "earliest"  # read from beginning if no committed offset
})

def is_fraud(txn):
    """Simple fraud rules."""
    try:
        if txn["amount"] > IMPOSSIBLE_AMOUNT:
            return True
        if txn["merchant"] in BLACKLIST_MERCHANTS:
            return True
        # Example: If event_id ends with 'X', treat as impossible travel
        if txn["event_id"].endswith("X"):
            return True
    except Exception:
        return False
    return False

def main():
    consumer.subscribe([TOPIC])

    print("🚀 Consumer started. Listening for transactions...\n")
    try:
        while True:
            msg = consumer.poll(1.0)  # wait up to 1s
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"❌ Consumer error: {msg.error()}")
                continue

            try:
                txn = json.loads(msg.value().decode("utf-8"))
                fraud_flag = is_fraud(txn)
                status = "🚨 FRAUD" if fraud_flag else "✅ Legit"
                print(f"{status} | {txn}")
            except json.JSONDecodeError:
                print(f"⚠️ Invalid JSON: {msg.value()}")

    except KeyboardInterrupt:
        print("\nStopping consumer…")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()

# import json
# import random
# import string
# import time
# from datetime import datetime, timezone
# from confluent_kafka import Producer

# BOOTSTRAP = "localhost:9094"
# TOPIC = "bank.transactions"

# accounts = [f"ACC{str(i).zfill(6)}" for i in range(1, 500)]
# merchants = [
#     "AMAZON", "UBER", "AIRBNB", "IKEA", "NETFLIX", "APPLE", "ZARA", "STARBUCKS",
#     "LOCAL_GROCERY", "WALMART", "BESTBUY", "ALIEXPRESS", "SHELL", "BP"
# ]
# # A few merchants we will treat as suspicious in the Flink job
# blacklist_merchants = ["SCAM_SHOP", "FAKE_STORE", "DARK_MARKET"]

# countries = ["PK", "AE", "US", "GB", "DE", "CN", "IN", "SG", "FR"]
# channels = ["POS", "ECOM", "ATM", "P2P"]

# producer = Producer({
#     "bootstrap.servers": BOOTSTRAP,
#     "client.id": "bank-txn-producer"
# })


# def random_amount():
#     # Mostly small values, occasional large spikes
#     base = random.choice([round(random.uniform(1, 200), 2) for _ in range(9)] +
#                          [round(random.uniform(2000, 15000), 2)])
#     return base


# def rand_txn(idx):
#     now = datetime.now(timezone.utc).isoformat()
#     acct = random.choice(accounts)

#     # normal merchant 90% of the time
#     if random.random() < 0.9:
#         merch = random.choice(merchants)
#     else:
#         merch = random.choice(blacklist_merchants)

#     txn = {
#         "event_id": f"EVT{idx:012d}",
#         "account_id": acct,
#         "amount": random_amount(),
#         "currency": "USD",
#         "merchant": merch,
#         "country": random.choice(countries),
#         "channel": random.choice(channels),
#         "timestamp": now
#     }

#     return txn


# def inject_fraud_patterns(batch):
#     """Mutate a few transactions to look blatantly fraudulent.
#     Patterns:
#     1) High amount spike > 8000
#     2) Impossible travel (country flip within ~30s) — we simulate by duplicating same account with new country
#     3) Blacklisted merchant
#     """
#     if not batch:
#         return batch

#     # 1) High amount
#     t = random.choice(batch)
#     t["amount"] = max(t["amount"], random.uniform(9000, 20000))

#     # 2) Impossible travel — clone one with different country
#     t2 = random.choice(batch)
#     clone = t2.copy()
#     clone["event_id"] = t2["event_id"] + "X"
#     # ensure different country
#     alt_countries = [c for c in countries if c != t2["country"]]
#     clone["country"] = random.choice(alt_countries)
#     batch.append(clone)

#     # 3) Blacklisted merchant
#     t3 = random.choice(batch)
#     t3["merchant"] = random.choice(["SCAM_SHOP", "FAKE_STORE", "DARK_MARKET"])  # suspicious

#     return batch


# def delivery_report(err, msg):
#     if err is not None:
#         print(f"❌ Delivery failed for {msg.key()}: {err}")
#     else:
#         # pass
#         print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


# def main():
#     i = 0
#     try:
#         while True:
#             # send a small burst each loop
#             batch = [rand_txn(i + j) for j in range(10)]
#             batch = inject_fraud_patterns(batch)

#             for j, txn in enumerate(batch):
#                 key = txn["account_id"].encode("utf-8")
#                 val = json.dumps(txn).encode("utf-8")
#                 producer.produce(TOPIC, key=key, value=val, callback=delivery_report)
#             producer.flush()

#             i += len(batch)
#             time.sleep(0.5)
#     except KeyboardInterrupt:
#         print("\nStopping producer…")
#     finally:
#         producer.flush()


# if __name__ == "__main__":
#     main()

import json
import random
import string
import time
from datetime import datetime, timezone
from confluent_kafka import Producer

BOOTSTRAP = "localhost:9094"
TOPIC = "bank.transactions"

accounts = [f"ACC{str(i).zfill(6)}" for i in range(1, 500)]
merchants = [
    "AMAZON", "UBER", "AIRBNB", "IKEA", "NETFLIX", "APPLE", "ZARA", "STARBUCKS",
    "LOCAL_GROCERY", "WALMART", "BESTBUY", "ALIEXPRESS", "SHELL", "BP"
]
# A few merchants we will treat as suspicious in the Flink job
blacklist_merchants = ["SCAM_SHOP", "FAKE_STORE", "DARK_MARKET"]

countries = ["PK", "AE", "US", "GB", "DE", "CN", "IN", "SG", "FR"]
channels = ["POS", "ECOM", "ATM", "P2P"]

producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "client.id": "bank-txn-producer"
})


def random_amount():
    # Mostly small values, occasional large spikes
    base = random.choice([round(random.uniform(1, 200), 2) for _ in range(9)] +
                         [round(random.uniform(2000, 15000), 2)])
    return base


def rand_txn(idx):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    acct = random.choice(accounts)

    # normal merchant 90% of the time
    if random.random() < 0.9:
        merch = random.choice(merchants)
    else:
        merch = random.choice(blacklist_merchants)

    txn = {
        "event_id": f"EVT{idx:012d}",
        "account_id": acct,
        "amount": random_amount(),
        "currency": "USD",
        "merchant": merch,
        "country": random.choice(countries),
        "channel": random.choice(channels),
        "timestamp": now
    }

    return txn


def inject_fraud_patterns(batch):
    """Mutate a few transactions to look blatantly fraudulent.
    Patterns:
    1) High amount spike > 8000
    2) Impossible travel (country flip within ~30s) — we simulate by duplicating same account with new country
    3) Blacklisted merchant
    """
    if not batch:
        return batch

    # 1) High amount
    t = random.choice(batch)
    t["amount"] = max(t["amount"], random.uniform(9000, 20000))

    # 2) Impossible travel — clone one with different country
    t2 = random.choice(batch)
    clone = t2.copy()
    clone["event_id"] = t2["event_id"] + "X"
    # ensure different country
    alt_countries = [c for c in countries if c != t2["country"]]
    clone["country"] = random.choice(alt_countries)
    clone["timestamp"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]+ "Z"
    batch.append(clone)

    # 3) Blacklisted merchant
    t3 = random.choice(batch)
    t3["merchant"] = random.choice(["SCAM_SHOP", "FAKE_STORE", "DARK_MARKET"])  # suspicious

    return batch


def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Delivery failed for {msg.key()}: {err}")
    else:
        print(f"✅ Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def main():
    i = 0
    try:
        while True:
            # send a small burst each loop
            batch = [rand_txn(i + j) for j in range(10)]
            batch = inject_fraud_patterns(batch)

            for j, txn in enumerate(batch):
                key = txn["account_id"].encode("utf-8")
                val = json.dumps(txn).encode("utf-8")
                producer.produce(TOPIC, key=key, value=val, callback=delivery_report)
            producer.flush()

            i += len(batch)
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nStopping producer…")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
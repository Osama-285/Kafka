import json
import time
import random
from confluent_kafka import Producer

BOOTSTRAP = "localhost:9094"   # adjust if needed
TOPIC = "transaction"

conf = {
    "bootstrap.servers": BOOTSTRAP,
    "client.id": "fraud-txn-producer"
}
producer = Producer(conf)

SKEW_CARD = "4111-1111-1111-1111"
CARDS = [SKEW_CARD] + [f"4000-0000-0000-{str(i).zfill(4)}" for i in range(2, 501)]

try:
    i = 0
    while True:
        i += 1
        if random.random() < 0.90:  # skew: stolen card used most
            card = SKEW_CARD
        else:
            card = random.choice(CARDS[1:])

        txn = {
            "txn_id": i,
            "card_number": card,
            "amount": round(random.uniform(1, 2000), 2),
            "merchant": random.choice(["Amazon", "eBay", "Walmart", "Target", "BestBuy"]),
            "ts": int(time.time() * 1000)
        }

        producer.produce(
            TOPIC,
            key=card,
            value=json.dumps(txn).encode("utf-8")
        )

        if i % 1000 == 0:
            producer.flush()
            print(f"Sent {i} transactions")

        time.sleep(0.001)  # ~1000 txns/sec
except KeyboardInterrupt:
    print("Stopping producer")
    producer.flush()

from confluent_kafka import Producer
from static_data import *
import pandas as pd
import json
import random
from datetime import datetime

conf = {
    'bootstrap.servers': 'localhost:9094',
    'client.id': 'port-event-batch-producer',
    'acks': 'all',
    'linger.ms': 500,
    'batch.num.messages': 10000
}
producer = Producer(conf)

def delivery_callback(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")

BATCH_SIZE = 10000
combined_records = []

for _ in range(BATCH_SIZE):
    now = random_past_datetime()
    ship = random.choice(ships)
    container_id = gen_container_id()
    ship_id = ship["shipId"]

    ship_event = {
        "shipId": ship_id,
        "imoNumber": ship["imoNumber"],
        "name": ship["name"],
        "flag": ship["flag"],
        "capacityTEU": ship["capacityTEU"],
        "totalContainers": random.randint(100, 500),
        "eventType": random.choice(ship_event_types),
        "berthId": random.choice(berths),
        "arrivalTime": now,
        "departureTime": None if random.random() < 0.5 else random_past_datetime(),
        "status": random.choice(ship_statuses),
        "timestamp": now
    }

    container_event = {
        "containerId": container_id,
        "shipId": ship_id,
        "size": random.choice(sizes),
        "weightKg": random.randint(5000, 30000),
        "sealNumber": gen_seal(),
        "eventType": random.choice(container_event_types),
        "location": random.choice(yards),
        "status": random.choice(container_statuses),
        "timestamp": now
    }

    category = random.choice(list(product_categories.keys()))
    product_name = random.choice(product_categories[category])
    product_event = {
        "productId": gen_product_id(),
        "containerId": container_id,
        "shipId": ship_id,
        "name": product_name,
        "category": category,
        "quantity": random.randint(1, 500),
        "weightKg": round(random.uniform(5.0, 500.0), 2),
        "status": random.choice(product_statuses),
        "timestamp": now
    }

    truck_event = {
        "truckId": gen_truck_id(),
        "driverId": gen_driver_id(),
        "containerId": container_id,
        "origin": random.choice(yards),
        "destination": random.choice(warehouses),
        "departureTime": now,
        "arrivalTime": None if random.random() < 0.3 else random_past_datetime(),
        "status": random.choice(truck_statuses)
    }

    staff_event = {
        "staffId": gen_staff_id(),
        "name": random.choice(names),
        "role": random.choice(staff_roles),
        "shift": random.choice(staff_shifts),
        "eventType": random.choice(staff_event_types),
        "timestamp": now
    }

    inspection_event = {
        "inspectionId": gen_inspection_id(),
        "containerId": container_id,
        "inspectorId": gen_staff_id(),
        "result": random.choice(inspection_results),
        "notes": random.choice(["No issues found", "Minor delay", "Suspicious cargo", "Cleared for delivery"]),
        "timestamp": now
    }

    producer.produce("shipEvent", key=ship_id, value=json.dumps(ship_event), callback=delivery_callback)
    producer.produce("containerEvent", key=ship_id, value=json.dumps(container_event), callback=delivery_callback)
    producer.produce("productEvent", key=ship_id, value=json.dumps(product_event), callback=delivery_callback)
    producer.produce("truckEvent", key=ship_id, value=json.dumps(truck_event), callback=delivery_callback)
    producer.produce("staffEvent", key=ship_id, value=json.dumps(staff_event), callback=delivery_callback)
    producer.produce("inspectionEvent", key=ship_id, value=json.dumps(inspection_event), callback=delivery_callback)

    record = {
        "shipId": ship_id,
        "containerId": container_id,

        **{f"ship_{k}": v for k, v in ship_event.items() if k not in ["shipId"]},
        **{f"container_{k}": v for k, v in container_event.items() if k not in ["containerId", "shipId"]},
        **{f"product_{k}": v for k, v in product_event.items() if k not in ["containerId", "shipId"]},
        **{f"truck_{k}": v for k, v in truck_event.items() if k not in ["containerId"]},
        **{f"staff_{k}": v for k, v in staff_event.items()},
        **{f"inspection_{k}": v for k, v in inspection_event.items() if k not in ["containerId"]},
    }

    combined_records.append(record)

producer.flush()
print("All events sent to Kafka successfully.")

df = pd.DataFrame(combined_records)
filename = f"port_events_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
df.to_csv(filename, index=False)
print(f"All events saved to {filename} with {len(df)} rows")

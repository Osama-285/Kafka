import random
import json
import time
from datetime import datetime, timedelta
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'localhost:9094',  # change if needed
    'client.id': 'mall-producer'
}
producer = Producer(conf)

# Topics for floors
FLOOR_TOPICS = {
    1: "mallFloor1",
    2: "mallFloor2",
    3: "mallFloor3",
    4: "mallFloor4",
    5: "mallFloor5"
}
SHOP_TYPES = [
    "Clothing", "Footwear", "Electronics", "FoodCourt", "Grocery",
    "Jewelry", "Toys", "Cosmetics", "Sports", "Books",
    "HomeDecor", "Cinema", "Pharmacy"
]

def random_date(start_year=2011):
    start = datetime(start_year, 1, 1)
    now = datetime.now()
    delta = now - start
    random_days = random.randint(0, delta.days)
    random_time = timedelta(seconds=random.randint(0, 86400))
    return (start + timedelta(days=random_days) + random_time).isoformat()

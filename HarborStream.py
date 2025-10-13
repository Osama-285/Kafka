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

def delivery_callback(err,msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [{msg.partition()}]")

ship_events, container_events, product_events = [], [], []
truck_events, staff_events, inspection_events = [], [], []

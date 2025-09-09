from confluent_kafka import Consumer, TopicPartition

conf = {
     'bootstrap.servers': 'localhost:9094',
    'group.id': 'test-direct-read',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

consumer.assign([TopicPartition('flight', 0)])

print("Reading from flightsPIA, partition 0")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
        else:
            print("Message:", msg.value().decode("utf-8"))
except KeyboardInterrupt:
    pass
finally:
    consumer.close()

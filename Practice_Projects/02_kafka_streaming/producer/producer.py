# Simple Kafka producer (uses kafka-python)
import json, time
from kafka import KafkaProducer
import random

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

topic = 'sales_events'
for i in range(1,101):
    event = {"order_id": i, "customer_id": random.randint(100,110), "quantity": random.randint(1,5), "price": round(random.random()*50,2), "timestamp": int(time.time())}
    producer.send(topic, event)
    print("sent", event)
    time.sleep(0.05)
producer.flush()

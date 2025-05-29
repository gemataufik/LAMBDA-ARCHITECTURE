from google.cloud import pubsub_v1
import json
import time
import os
from faker import Faker
import random


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "purwadika-key.json"

project_id = "purwadika"
topic_id = "capstone3_topic_gema"
topic_path = f"projects/{project_id}/topics/{topic_id}"

publisher = pubsub_v1.PublisherClient()
fake = Faker()

def generate_taxi_message():
    message = {
        "driver_id": str(random.randint(1000, 9999)),
        "driver_name": fake.name(),
        "fare_amount": round(random.uniform(5, 100), 2),
        "trip_start_timestamp": fake.iso8601(), 
        "payment_type": random.choice(["cash", "credit_card", "other"])
    }
    return message

for _ in range(2):  
    msg = generate_taxi_message()
    data_str = json.dumps(msg)
    data = data_str.encode("utf-8")
    future = publisher.publish(topic_path, data=data)
    print(f"Published message ID: {future.result()} with data: {msg}")
    time.sleep(1)

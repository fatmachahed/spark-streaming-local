# Write your kafka producer code here
import json
import time
import random
from kafka import KafkaProducer

TOPIC = "events-topic"
BOOTSTRAP_SERVERS = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def generate_event():
    return {
        "user_id": f"user_{random.randint(1, 5)}",
        "event_type": random.choice(["click", "purchase", "view"]),
        "amount": random.randint(10, 500),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")
    }

if __name__ == "__main__":
    print("Starting Kafka Producer...")

    while True:
        event = generate_event()
        print("Sending:", event)
        producer.send(TOPIC, value=event)
        time.sleep(2)
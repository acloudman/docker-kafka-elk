from faker import Faker
from kafka import KafkaProducer
import json
import time

fake = Faker()


def get_registered_user():
    return {"name": fake.name(), "address": fake.address(), "created_at": fake.year()}


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"], value_serializer=json_serializer
)

if __name__ == "__main__":
    while 1 == 1:
        registered_user = get_registered_user()
        print("-" * 50)
        print(registered_user)
        print("-" * 50)
        producer.send("registered-users", registered_user)
        time.sleep(4)

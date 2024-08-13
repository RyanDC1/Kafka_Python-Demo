import json
import time

from kafka import KafkaProducer, producer
from constants import Topics

order_limit = 15

producer = KafkaProducer(bootstrap_servers="localhost:29092")


for i in range(1, order_limit):
    data = {
        "order_id": i,
        "user_id": f"tom_{i}",
        "total_cost": i * 13,
        "items": "Burger, Pizza"
    }

    producer.send(
        Topics.orders,
        json.dumps(data).encode("utf-8")
    )

    print(f"order completed: {i}")
    time.sleep(5)
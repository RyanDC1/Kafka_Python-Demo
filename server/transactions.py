import json

from kafka import KafkaConsumer
from kafka import KafkaProducer

from constants import Topics


consumer = KafkaConsumer(
    Topics.orders,
    bootstrap_servers="localhost:29092"
)

producer = KafkaProducer(
    bootstrap_servers="localhost:29092"
)

print("Listening for transactions...")

while True:
    for message in consumer:
        consumed_message = json.loads(message.value.decode())
        print(consumed_message)

        user_id = consumed_message["user_id"]
        total_cost = consumed_message["total_cost"]

        data = {
            "customer_id": user_id,
            "customer_email": f"{user_id}@gmail.com",
            "total_cost": total_cost
        }

        print("Success")
        producer.send(
            Topics.confirmed_orders,
            json.dumps(data).encode("utf-8")
        )
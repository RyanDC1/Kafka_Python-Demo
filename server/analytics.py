import json
from kafka import KafkaConsumer

from constants import Topics

consumer = KafkaConsumer(
    Topics.confirmed_orders,
    bootstrap_servers="localhost:29092"
)

total_orders_count = 0
revenue = 0

print("Monitoring Orders")

while True:
    for message in consumer:
        print("Processing...")
        consumed_message = json.loads(message.value.decode())

        order_cost = float(consumed_message["total_cost"])

        total_orders_count += 1
        revenue += order_cost

        print(f"updated revenue: {revenue}")

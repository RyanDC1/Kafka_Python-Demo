import json
from kafka import KafkaConsumer

from constants import Topics

consumer = KafkaConsumer(
    Topics.confirmed_orders,
    bootstrap_servers="localhost:29092"
)

emails = set()

while True:
    for message in consumer:
        consumed_message = json.loads(message.value.decode())
        customer_email = consumed_message["customer_email"]

        print(f"Sending email to {customer_email}")

        emails.add(customer_email)
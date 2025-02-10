import json
import logging
import os
import random
import sys
import time

from confluent_kafka import Producer
from dotenv import load_dotenv
from faker import Faker

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from config.config import KAFKA_BROKER, TOPIC

load_dotenv()

producer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'client.id': 'customer-producer'
}
producer = Producer(producer_conf)
faker = Faker()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def generate_customer():
    """Generates fake customer data."""
    return {
        "first_name": faker.first_name(),
        "last_name": faker.last_name(),
        "age": random.randint(18, 70),
        "email": faker.email(),
        "country": faker.country(),
        "city": faker.city(),
        "postal_code": faker.zipcode(),
        "phone_number": faker.phone_number(),
        "registration_date": faker.date_time_this_decade().isoformat(),
        "last_login_date": faker.date_time_this_year().isoformat()
    }


def delivery_report(err, msg):
    """Feedback function when sending a message."""
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def produce_messages(batch_size=10):
    """Sends fake customers to Kafka in batches."""
    while True:
        batch = []
        for _ in range(batch_size):
            try:
                customer_json = json.dumps(generate_customer())
                batch.append(customer_json)
            except (TypeError, ValueError) as e:
                logging.error(f"Serialization error: {e}")

        for customer_json in batch:
            try:
                producer.produce(TOPIC, value=customer_json, callback=delivery_report)
            except Exception as e:
                logging.error(f"Kafka produce error: {e}")

        producer.flush()  # Ensure all messages are sent before the next batch
        time.sleep(random.uniform(1, 3))


if __name__ == "__main__":
    try:
        logging.info("Kafka Producer is running...")
        produce_messages()
    except KeyboardInterrupt:
        logging.info("Producer stopped.")

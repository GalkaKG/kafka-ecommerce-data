import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Optional

import psycopg2
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv
from pydantic import BaseModel, EmailStr, field_validator

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from config.config import DB_PARAMS, KAFKA_BROKER, TOPIC, GROUP_ID
from src.utils.utils import retry_db_operation

load_dotenv()

consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)
consumer.subscribe([TOPIC])

producer = Producer({'bootstrap.servers': KAFKA_BROKER})
DLQ_TOPIC = "customers_dlq"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def retry_kafka_operation(func, retries=5, delay=2):
    for attempt in range(retries):
        try:
            return func()
        except Exception as e:
            logging.error(f"Kafka operation failed (attempt {attempt + 1}/{retries}): {e}")
            time.sleep(delay)
    logging.error("Kafka operation failed after max retries.")


def send_to_dlq(message, error):
    dlq_message = {"error": str(error), "original_message": message}
    producer.produce(DLQ_TOPIC, json.dumps(dlq_message))
    logging.error(f"Sent bad message to DLQ: {message}")


class CustomerSchema(BaseModel):
    first_name: str
    last_name: str
    age: Optional[int]
    email: EmailStr
    country: str
    city: str
    postal_code: str
    phone_number: Optional[str]
    registration_date: Optional[datetime]
    last_login_date: Optional[datetime]

    @staticmethod
    @field_validator("phone_number", mode="before")
    def clean_phone(cls, v):
        return ''.join(filter(str.isdigit, v)) if v else None


def transform_customer_data(customer):
    """Validate and transform customer data."""
    try:
        validated_customer = CustomerSchema(**customer).model_dump()
        return validated_customer
    except Exception as e:
        logging.error(f"Data validation error: {e}")
        return None


def insert_customer_to_db(customer):
    """Insert customer to the database with retry logic."""
    if not customer:
        return

    def db_insert():
        try:
            with psycopg2.connect(**DB_PARAMS) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO customers (first_name, last_name, age, email, country, city, postal_code, /
                        phone_number, registration_date, last_login_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (email) DO NOTHING;
                    """, (
                        customer.get("first_name"), customer.get("last_name"), customer.get("age"),
                        customer.get("email"), customer.get("country"), customer.get("city"),
                        customer.get("postal_code"), customer.get("phone_number"),
                        customer.get("registration_date"), customer.get("last_login_date")
                    ))
            logging.info(f"Inserted customer: {customer['email']}")
        except Exception as e:
            logging.error(f"Database error: {e}")

    retry_db_operation(db_insert)


def consume_messages():
    """Consume messages from Kafka topic."""
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logging.error(f"Kafka error: {msg.error()}")
                break
            try:
                messages = msg.value().decode('utf-8').strip().split("\n")
                for message in messages:
                    try:
                        customer = json.loads(message)
                        transformed_customer = transform_customer_data(customer)
                        if transformed_customer:
                            insert_customer_to_db(transformed_customer)
                        else:
                            send_to_dlq(message, "Validation failed")
                    except (json.JSONDecodeError, TypeError) as e:
                        logging.error(f"Error processing message: {e}. Raw message: {message}")
                        send_to_dlq(message, e)
            except Exception as e:
                logging.error(f"Unexpected error: {e}")
    except KeyboardInterrupt:
        logging.info("Consumer stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    logging.info("Kafka Consumer is running...")
    consume_messages()

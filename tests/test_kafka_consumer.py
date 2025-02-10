import json
import os
import sys
import unittest
from unittest import mock
from unittest.mock import MagicMock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from consumer.kafka_consumer import is_valid_email, transform_customer_data, insert_customer_to_db, consume_messages


# Mocking Psycopg2 connection and cursor for database operations
class TestConsumer(unittest.TestCase):

    @mock.patch('consumer.kafka_consumer.psycopg2')
    def test_insert_customer_to_db(self, mock_psycopg2):
        print("Running test_insert_customer_to_db")
        # Setup mock connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Test customer data
        customer = {
            "first_name": "John",
            "last_name": "Doe",
            "age": 30,
            "email": "john.doe@example.com",
            "country": "US",
            "city": "New York",
            "postal_code": "10001",
            "phone_number": "(123) 456-7890",
            "registration_date": "2022-01-01T00:00:00",
            "last_login_date": "2022-02-01T00:00:00"
        }

        insert_customer_to_db(customer)

        # Check if the database insert was called with the correct parameters
        mock_cursor.execute.assert_called_with(
            """INSERT INTO customers (first_name, last_name, age, email, country, city, postal_code, phone_number, registration_date, last_login_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (email) DO NOTHING;""", (
                "John", "Doe", 30, "john.doe@example.com", "US", "New York", "10001", "1234567890",
                "2022-01-01T00:00:00", "2022-02-01T00:00:00"
            ))

        # Check if the commit was called
        mock_conn.commit.assert_called_once()

    def test_is_valid_email(self):
        # Valid email test cases
        valid_emails = [
            "test@example.com",
            "valid_123@example.co.uk",
            "valid.email@example.com"
        ]

        for email in valid_emails:
            self.assertTrue(is_valid_email(email))

        # Invalid email test cases
        invalid_emails = [
            "invalid-email",
            "missing@domain",
            "@missing.local",
            "missingdomain@.com"
        ]

        for email in invalid_emails:
            self.assertFalse(is_valid_email(email))

    def test_transform_customer_data(self):
        customer = {
            "first_name": "John",
            "last_name": "Doe",
            "email": " JOHN.DOE@Example.com  ",
            "phone_number": "(123) 456-7890",
            "registration_date": "2022-01-01T00:00:00",
            "last_login_date": "2022-02-01T00:00:00"
        }

        transformed_customer = transform_customer_data(customer)

        # Check if email is lowercased and stripped
        self.assertEqual(transformed_customer["email"], "john.doe@example.com")
        # Check if phone number is cleaned
        self.assertEqual(transformed_customer["phone_number"], "1234567890")
        # Check if dates are transformed correctly
        self.assertEqual(transformed_customer["registration_date"].isoformat(), "2022-01-01T00:00:00")
        self.assertEqual(transformed_customer["last_login_date"].isoformat(), "2022-02-01T00:00:00")

    @mock.patch('consumer.kafka_consumer.logging')
    @mock.patch('consumer.kafka_consumer.psycopg2')
    @mock.patch('consumer.kafka_consumer.Consumer')
    def test_consume_messages(self, mock_KafkaConsumer, mock_psycopg2, mock_logging):
        # Setup mock Kafka consumer
        mock_consumer = MagicMock()
        mock_KafkaConsumer.return_value = mock_consumer
        mock_message = MagicMock()

        # Define the message structure you expect from Kafka
        mock_message.value.return_value = json.dumps({
            "first_name": "John",
            "last_name": "Doe",
            "email": "john.doe@example.com",
            "phone_number": "(123) 456-7890",
            "registration_date": "2022-01-01T00:00:00",
            "last_login_date": "2022-02-01T00:00:00"
        }).encode('utf-8')  # Encode to bytes

        # Mock the decode method
        mock_message.value.decode.return_value = json.dumps({
            "first_name": "John",
            "last_name": "Doe",
            "email": "john.doe@example.com",
            "phone_number": "(123) 456-7890",
            "registration_date": "2022-01-01T00:00:00",
            "last_login_date": "2022-02-01T00:00:00"
        })

        # Mock the poll method to return the message
        mock_consumer.poll.return_value = mock_message

        # Setup mock for psycopg2 connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_psycopg2.connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Run the consume_messages function
        consume_messages()

        # Check that the consume_messages function logged the correct message
        mock_logging.info.assert_any_call("Kafka Consumer is running...")
        mock_logging.info.assert_any_call("Inserted customer: john.doe@example.com")

        # Check if poll was called on the consumer
        mock_consumer.poll.assert_called_once()

        # Check that we are correctly calling decode on the message value
        mock_message.value.decode.assert_called_once()

        # Check that the customer data is transformed and inserted into the DB
        mock_cursor.execute.assert_called_with(
            """INSERT INTO customers (first_name, last_name, age, email, country, city, postal_code, phone_number, registration_date, last_login_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (email) DO NOTHING;""", (
                "John", "Doe", 30, "john.doe@example.com", "US", "New York", "10001",
                "1234567890", "2022-01-01T00:00:00", "2022-02-01T00:00:00"
            ))

        # Check that the commit was called
        mock_conn.commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()

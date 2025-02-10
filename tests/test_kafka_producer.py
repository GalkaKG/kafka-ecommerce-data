import os
import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from producer.kafka_producer import generate_customer, delivery_report, produce_messages


class TestProducer(unittest.TestCase):

    @patch('producer.kafka_producer.Faker')
    def test_generate_customer(self, MockFaker):
        # Mocking Faker to return controlled values
        mock_faker = MockFaker.return_value
        mock_faker.first_name.return_value = "John"
        mock_faker.last_name.return_value = "Doe"
        mock_faker.email.return_value = "john.doe@example.com"
        mock_faker.country.return_value = "Countryland"
        mock_faker.city.return_value = "Cityville"
        mock_faker.zipcode.return_value = "12345"
        mock_faker.phone_number.return_value = "123-456-7890"
        mock_faker.date_time_this_decade.return_value.isoformat.return_value = "2015-01-01T12:00:00"
        mock_faker.date_time_this_year.return_value.isoformat.return_value = "2025-02-01T09:00:00"

        customer = generate_customer()

        # Check if the generated customer data matches the mocked values
        self.assertEqual(customer["first_name"], "John")
        self.assertEqual(customer["last_name"], "Doe")
        self.assertEqual(customer["email"], "john.doe@example.com")
        self.assertEqual(customer["country"], "Countryland")
        self.assertEqual(customer["city"], "Cityville")
        self.assertEqual(customer["postal_code"], "12345")
        self.assertEqual(customer["phone_number"], "123-456-7890")
        self.assertEqual(customer["registration_date"], "2015-01-01T12:00:00")
        self.assertEqual(customer["last_login_date"], "2025-02-01T09:00:00")

    @patch('producer.kafka_producer.Producer')
    def test_delivery_report_success(self, MockProducer):
        # Mocking the producer's delivery report
        mock_msg = MagicMock()
        mock_msg.topic.return_value = 'test-topic'
        mock_msg.partition.return_value = 0

        with self.assertLogs('producer', level='INFO') as log:
            delivery_report(None, mock_msg)

        self.assertIn("INFO:producer:Message delivered to test-topic [0]", log.output[0])

    @patch('producer.kafka_producer.Producer')
    def test_delivery_report_failure(self, MockProducer):
        # Mocking the producer's delivery report with an error
        with self.assertLogs('producer', level='ERROR') as log:
            delivery_report(Exception("Some error"), None)

        self.assertIn("ERROR:producer:Message delivery failed: Some error", log.output[0])

    @patch('producer.kafka_producer.Producer')
    @patch('producer.kafka_producer.time.sleep')
    def test_produce_messages(self, MockSleep, MockProducer):
        # Mocking produce to simulate Kafka interaction
        mock_producer = MockProducer.return_value
        mock_producer.produce = MagicMock()
        mock_producer.flush = MagicMock()

        # Mock generate_customer to return a static value
        with patch('producer.kafka_producer.generate_customer',
                   return_value={"email": "test@example.com", "first_name": "John"}):
            produce_messages(max_messages=3)  # Limit to 3 messages

        # Check if producer.produce was called 3 times
        self.assertEqual(mock_producer.produce.call_count, 3)
        # Check if flush was called to send messages
        mock_producer.flush.assert_called()

    @patch('producer.kafka_producer.Producer')
    @patch('producer.kafka_producer.time.sleep', return_value=None)  # Mock sleep to avoid delay
    def test_produce_messages_infinite_loop(self, MockSleep, MockProducer):
        # Limit the number of iterations to test infinite loop behavior
        mock_producer = MockProducer.return_value
        mock_producer.produce = MagicMock()
        mock_producer.flush = MagicMock()

        # Mock generate_customer to return a static value
        with patch('producer.kafka_producer.generate_customer',
                   return_value={"email": "test@example.com", "first_name": "John"}):
            produce_messages(max_messages=3)  # Limit to 3 messages

        self.assertEqual(mock_producer.produce.call_count, 3)
        mock_producer.flush.assert_called()

    @patch('producer.kafka_producer.Producer')
    @patch('producer.kafka_producer.time.sleep', return_value=None)  # Mock sleep to avoid delay
    def test_produce_messages_infinite_loop(self, MockSleep, MockProducer):
        # Limit the number of iterations to test infinite loop behavior
        mock_producer = MockProducer.return_value
        mock_producer.produce = MagicMock()
        mock_producer.flush = MagicMock()

        # Mock generate_customer to return a static value
        with patch('producer.kafka_producer.generate_customer',
                   return_value={"email": "test@example.com", "first_name": "John"}):
            # Let's produce only 3 messages for testing
            for _ in range(3):
                produce_messages()

        self.assertEqual(mock_producer.produce.call_count, 3)
        mock_producer.flush.assert_called()


if __name__ == '__main__':
    unittest.main()

# Task EGT Digital Project

## Overview

The **Task EGT Digital** project is a data processing application designed to handle and process e-commerce data, interact with Kafka, and generate data for testing purposes. The project includes components for both consuming and producing Kafka messages, generating test data, and interacting with a database schema for e-commerce applications.

## Directory Structure

The project follows a structured directory layout for better organization and maintainability:

- `config/`: Configuration files and environment variables.
  - `config.py`: Main configuration script.
  - `.env`: Environment variables for the project.

- `docker/`: Docker-related files for containerization and orchestration.
  - `docker-compose.yml`: Docker Compose configuration for running the application in containers.

- `env/`: Python virtual environment setup.

- `migrations/`: SQL migration files for database setup.
  - `init_ecommerce_db.sql`: Initial SQL schema for the e-commerce database.

- `src/`: Source code for the project.
  - `consumer/`: Contains the Kafka consumer logic.
    - `kafka_consumer.py`: Script for consuming messages from Kafka.
  - `producer/`: Contains the Kafka producer logic.
    - `generate_data.py`: Data generation script for producing Kafka messages.
    - `kafka_producer.py`: Script for sending messages to Kafka.
  - `utils/`: Utility scripts for common functions.
    - `utils.py`: Helper functions.

- `tests/`: Unit tests and test scripts for the project.
  - `test_data_validation.py`: Tests for data validation functions.
  - `test_kafka_consumer.py`: Tests for Kafka consumer functionality.
  - `test_kafka_producer.py`: Tests for Kafka producer functionality.

- `.idea/`: IDE configuration files (e.g., for PyCharm).

- `requirements.txt`: List of Python dependencies for the project.

- `Task_EGT_Digital.zip`: A zip file of the project for distribution or backup purposes.

## Key Features

- **Kafka Consumer and Producer**: The project implements Kafka consumer and producer scripts for data processing and message handling.
- **Data Generation**: Utility for generating sample data to be sent to Kafka.
- **Database Migration**: SQL script for initializing an e-commerce database schema.
- **Testing**: Unit tests are provided for core functionality, ensuring the reliability of the Kafka consumer, producer, and data validation components.

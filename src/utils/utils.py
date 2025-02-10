import logging
import time


def retry_db_operation(operation, retries=3, delay=1):
    """Retry a database operation with specified retries and delay."""
    for _ in range(retries):
        try:
            # Call the operation (e.g., insert customer to DB)
            operation()
            return  # Exit if successful
        except Exception as e:
            logging.error(f"Database error: {e}. Retrying...")
            time.sleep(delay)  # Wait before retrying
    logging.error(f"Failed after {retries} retries.")

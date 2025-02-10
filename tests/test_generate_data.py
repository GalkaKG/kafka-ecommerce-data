import os
import sys
import unittest

import psycopg2

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src/producer')))

from generate_data import generate_customers, generate_suppliers, generate_products, generate_orders, generate_reviews, \
    NUM_CUSTOMERS, NUM_SUPPLIERS, NUM_PRODUCTS, NUM_ORDERS, NUM_REVIEWS, CATEGORIES, ORDER_STATUSES
from config.config import DB_PARAMS


class TestDataGeneration(unittest.TestCase):

    def test_generate_customers(self):
        customers = generate_customers()

        # Test if we get the correct number of customers
        self.assertEqual(len(customers), NUM_CUSTOMERS)

        # Test if the customer data is valid
        for customer in customers:
            self.assertEqual(len(customer), 10)  # Expect 10 attributes
            self.assertTrue(isinstance(customer[0], str))  # first_name
            self.assertTrue(isinstance(customer[1], str))  # last_name
            self.assertTrue(isinstance(customer[3], str))  # email
            self.assertTrue(isinstance(customer[4], str))  # country
            self.assertTrue(isinstance(customer[5], str))  # city

    def test_generate_suppliers(self):
        suppliers = generate_suppliers()

        # Test if we get the correct number of suppliers
        self.assertEqual(len(suppliers), NUM_SUPPLIERS)

        # Test if supplier data is valid
        for supplier in suppliers:
            self.assertEqual(len(supplier), 6)  # Expect 6 attributes
            self.assertTrue(isinstance(supplier[0], str))  # company_name
            self.assertTrue(isinstance(supplier[1], str))  # contact_name
            self.assertTrue(isinstance(supplier[2], str))  # email

    def test_generate_products(self):
        # Fake some supplier IDs
        supplier_ids = [1, 2, 3]
        products = generate_products(supplier_ids)

        # Test if we get the correct number of products
        self.assertEqual(len(products), NUM_PRODUCTS)

        # Test if each product has a valid category
        for product in products:
            self.assertTrue(product[1] in CATEGORIES)  # category should be in predefined categories
            self.assertTrue(isinstance(product[3], float))  # price should be a float

    def test_generate_orders(self):
        # Fake some customer IDs
        customer_ids = [1, 2, 3]
        orders = generate_orders(customer_ids)

        # Test if we get the correct number of orders
        self.assertEqual(len(orders), NUM_ORDERS)

        # Test if each order has a valid status
        for order in orders:
            self.assertTrue(order[3] in ORDER_STATUSES)  # status should be in predefined order statuses

    def test_generate_reviews(self):
        # Fake some customer and product IDs
        customer_ids = [1, 2, 3]
        product_ids = [1, 2, 3]
        reviews = generate_reviews(customer_ids, product_ids)

        # Test if we get the correct number of reviews
        self.assertEqual(len(reviews), NUM_REVIEWS)

        # Test if each review has a valid rating
        for review in reviews:
            self.assertTrue(1 <= review[2] <= 5)  # rating should be between 1 and 5

    def test_db_connection(self):

        try:
            conn = psycopg2.connect(**DB_PARAMS)
            cursor = conn.cursor()
            cursor.execute('SELECT 1')
            result = cursor.fetchone()
            self.assertEqual(result[0], 1, "Database connection failed")
            cursor.close()
            conn.close()
        except Exception as e:
            self.fail(f"Failed to connect to the database: {e}")


if __name__ == "__main__":
    unittest.main()

import os
import random
import sys
from datetime import timedelta

import psycopg2
from faker import Faker

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from config.config import DB_PARAMS


def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)


fake = Faker()

# Constants
NUM_CUSTOMERS = 1000
NUM_SUPPLIERS = 50
NUM_PRODUCTS = 500
NUM_ORDERS = 2000
MAX_ORDER_ITEMS = 5
NUM_REVIEWS = 1500
CURRENCIES = ['BGN', 'EUR', 'USD', 'GBP']
ORDER_STATUSES = ['Pending', 'Shipped', 'Delivered', 'Cancelled']
PAYMENT_METHODS = ['Credit Card', 'PayPal', 'Bank Transfer', 'Cash on Delivery']
CATEGORIES = ['Electronics', 'Clothing', 'Home', 'Toys', 'Sports']


# Insert customers
def generate_customers():
    customers = []
    for _ in range(NUM_CUSTOMERS):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.unique.email()
        phone = fake.unique.phone_number()
        age = random.randint(18, 80)
        country = fake.country()
        city = fake.city()
        postal_code = fake.postcode()
        registration_date = fake.date_time_this_decade()
        last_login_date = registration_date + timedelta(days=random.randint(0, 365))
        customers.append(
            (first_name, last_name, age, email, country, city, postal_code, phone, registration_date, last_login_date))
    return customers


# Insert suppliers
def generate_suppliers():
    suppliers = []
    for _ in range(NUM_SUPPLIERS):
        suppliers.append(
            (fake.company(), fake.name(), fake.email(), fake.phone_number(), fake.country(), random.randint(1, 30)))
    return suppliers


# Insert products
def generate_products(supplier_ids):
    products = []
    for _ in range(NUM_PRODUCTS):
        category = random.choice(CATEGORIES)
        subcategory = category + " Sub"
        price = round(random.uniform(5, 500), 2)
        cost = round(price * random.uniform(0.5, 0.9), 2)
        stock_quantity = random.randint(0, 100)
        weight = round(random.uniform(0.1, 5.0), 3)
        dimensions = f"{random.randint(5, 100)}x{random.randint(5, 100)}x{random.randint(1, 50)} cm"
        supplier_id = random.choice(supplier_ids)
        products.append(
            (fake.word(), category, subcategory, price, cost, supplier_id, stock_quantity, weight, dimensions))
    return products


# Insert orders
def generate_orders(customer_ids):
    orders = []
    for _ in range(NUM_ORDERS):
        customer_id = random.choice(customer_ids)
        order_date = fake.date_time_this_year()
        status = random.choice(ORDER_STATUSES)
        total_amount = round(random.uniform(10, 1000), 2)
        shipping_address = fake.address()
        payment_method = random.choice(PAYMENT_METHODS)
        currency = random.choice(CURRENCIES)
        orders.append((customer_id, order_date, total_amount, status, shipping_address, payment_method, currency))
    return orders


# Insert order items
def generate_order_items(order_ids, product_ids):
    order_items = []
    for order_id in order_ids:
        num_items = random.randint(1, MAX_ORDER_ITEMS)
        selected_products = random.sample(product_ids, num_items)
        for product_id in selected_products:
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(5, 500), 2)
            discount = round(unit_price * random.uniform(0, 0.3), 2)
            total_price = round((unit_price - discount) * quantity, 2)
            order_items.append((order_id, product_id, quantity, unit_price, discount, total_price))
    return order_items


# Insert product reviews
def generate_reviews(customer_ids, product_ids):
    reviews = []
    for _ in range(NUM_REVIEWS):
        reviews.append((random.choice(product_ids), random.choice(customer_ids), random.randint(1, 5), fake.sentence(),
                        fake.date_time_this_year()))
    return reviews


# Main function to insert data
def insert_data():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO customers (first_name, last_name, age, email, country, city, postal_code, phone_number, registration_date, last_login_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, generate_customers())

            cur.executemany(
                "INSERT INTO suppliers (company_name, contact_name, email, phone, country, lead_time) VALUES (%s, %s, %s, %s, %s, %s)",
                generate_suppliers())

            cur.execute("SELECT supplier_id FROM suppliers")
            supplier_ids = [row[0] for row in cur.fetchall()]
            cur.executemany(
                "INSERT INTO products (name, category, subcategory, price, cost, supplier_id, stock_quantity, weight, dimensions) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                generate_products(supplier_ids))

            cur.execute("SELECT customer_id FROM customers")
            customer_ids = [row[0] for row in cur.fetchall()]
            cur.executemany(
                "INSERT INTO orders (customer_id, order_date, total_amount, status, shipping_address, payment_method, currency) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                generate_orders(customer_ids))

            cur.execute("SELECT order_id FROM orders")
            order_ids = [row[0] for row in cur.fetchall()]
            cur.execute("SELECT product_id FROM products")
            product_ids = [row[0] for row in cur.fetchall()]
            cur.executemany(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price, discount_amount, total_price) VALUES (%s, %s, %s, %s, %s, %s)",
                generate_order_items(order_ids, product_ids))

            cur.executemany(
                "INSERT INTO product_reviews (product_id, customer_id, rating, review_text, review_date) VALUES (%s, %s, %s, %s, %s)",
                generate_reviews(customer_ids, product_ids))

            conn.commit()


if __name__ == "__main__":
    insert_data()

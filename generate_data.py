import os
from dotenv import load_dotenv
from faker import Faker
import faker_commerce
import psycopg2
from psycopg2 import sql

# Load environment variables
load_dotenv()

# Initialize Faker
fake = Faker()
fake.add_provider(faker_commerce.Provider)

# Database connection parameters
db_params = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

# Create table if not exists
create_table_query = """
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE,
    name VARCHAR(100),
    description TEXT,
    price DECIMAL(10, 2),
    category VARCHAR(50),
    stock INTEGER,
    creation_date DATE,
    company VARCHAR(100),
    company_email VARCHAR(100),
    origin_country VARCHAR(2)
);
"""

# Insert product query
insert_product_query = """
INSERT INTO products (code, name, description, price, category, stock, creation_date, company, company_email, origin_country)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (code) DO NOTHING
RETURNING id;
"""

def create_connection():
    return psycopg2.connect(**db_params)

def create_table(conn):
    with conn.cursor() as cur:
        cur.execute(create_table_query)
    conn.commit()

def get_products(num_products):
    products = []
    for _ in range(num_products):
        product = (
            fake.unique.ean13(),
            fake.ecommerce_name(),
            fake.sentence(),
            round(fake.random.uniform(10, 1000), 2),
            fake.ecommerce_category(),
            fake.ecommerce_price(False),
            fake.date_this_year(),
            fake.company(),
            fake.company_email(),
            fake.country_code(representation="alpha-2")
        )
        products.append(product)
        print(product)
    return products

def insert_products(conn, num_products):
    inserted_count = 0
    products = get_products(num_products)
    with conn.cursor() as cur:
        for product in products:
            cur.execute(insert_product_query, product)
            if cur.fetchone() is not None:
                inserted_count += 1
    conn.commit()
    return inserted_count

def main():
    conn = create_connection()
    try:
        create_table(conn)
        inserted = insert_products(conn, 250)
        print(f"Successfully inserted {inserted} new products.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
    #get_products(10)
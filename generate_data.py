import pandas as pd
from faker import Faker
import random
import os
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()
Faker.seed(42)
random.seed(42)

# Configuration
NUM_CUSTOMERS = 100
NUM_PRODUCTS = 50
NUM_ORDERS = 500
OUTPUT_DIR = 'data'

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def generate_customers(n):
    customers = []
    for _ in range(n):
        customers.append({
            'customer_id': fake.uuid4(),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'address': fake.address().replace('\n', ', '),
            'city': fake.city(),
            'state': fake.state(),
            'zip_code': fake.zipcode(),
            'created_at': fake.date_time_between(start_date='-2y', end_date='now')
        })
    return pd.DataFrame(customers)

def generate_products(n):
    products = []
    categories = ['Electronics', 'Books', 'Clothing', 'Home', 'Toys']
    for _ in range(n):
        products.append({
            'product_id': fake.uuid4(),
            'product_name': fake.catch_phrase(),
            'category': random.choice(categories),
            'price': round(random.uniform(10.0, 500.0), 2),
            'cost': round(random.uniform(5.0, 250.0), 2) # For profit calc
        })
    return pd.DataFrame(products)

def generate_orders(n, customer_ids):
    orders = []
    for _ in range(n):
        order_date = fake.date_time_between(start_date='-1y', end_date='now')
        orders.append({
            'order_id': fake.uuid4(),
            'customer_id': random.choice(customer_ids),
            'order_date': order_date,
            'status': random.choice(['Completed', 'Shipped', 'Processing', 'Cancelled'])
        })
    return pd.DataFrame(orders)

def generate_order_items(orders_df, products_df):
    order_items = []
    product_ids = products_df['product_id'].tolist()
    
    for _, order in orders_df.iterrows():
        # Randomly assign 1-5 items per order
        num_items = random.randint(1, 5)
        selected_products = random.sample(product_ids, num_items)
        
        for prod_id in selected_products:
            product = products_df[products_df['product_id'] == prod_id].iloc[0]
            quantity = random.randint(1, 3)
            order_items.append({
                'order_item_id': fake.uuid4(),
                'order_id': order['order_id'],
                'product_id': prod_id,
                'quantity': quantity,
                'unit_price': product['price'] # Price at time of purchase
            })
    return pd.DataFrame(order_items)

def main():
    print("Generating Customers...")
    df_customers = generate_customers(NUM_CUSTOMERS)
    df_customers.to_csv(f'{OUTPUT_DIR}/customers.csv', index=False)
    
    print("Generating Products...")
    df_products = generate_products(NUM_PRODUCTS)
    df_products.to_csv(f'{OUTPUT_DIR}/products.csv', index=False)
    
    print("Generating Orders...")
    df_orders = generate_orders(NUM_ORDERS, df_customers['customer_id'].tolist())
    df_orders.to_csv(f'{OUTPUT_DIR}/orders.csv', index=False)
    
    print("Generating Order Items...")
    df_order_items = generate_order_items(df_orders, df_products)
    df_order_items.to_csv(f'{OUTPUT_DIR}/order_items.csv', index=False)
    
    print(f"Data generation complete. Files saved to '{OUTPUT_DIR}/'")

if __name__ == "__main__":
    main()

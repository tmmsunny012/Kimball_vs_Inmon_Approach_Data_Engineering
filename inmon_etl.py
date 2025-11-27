import pandas as pd
import sqlite3
import os
from datetime import datetime

# Configuration
DATA_DIR = 'data'
DB_NAME = 'inmon_edw.db'

def create_connection():
    conn = None
    try:
        conn = sqlite3.connect(DB_NAME)
        print(f"Connected to {DB_NAME}")
    except Exception as e:
        print(e)
    return conn

def create_tables(conn):
    cursor = conn.cursor()
    
    # Inmon Approach: Normalized (3NF) Tables
    # Focus on atomic data, data integrity, and neutral storage
    
    # Customer Table (3NF)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Customer (
            Customer_ID TEXT PRIMARY KEY,
            First_Name TEXT,
            Last_Name TEXT,
            Email TEXT,
            Address TEXT,
            City TEXT,
            State TEXT,
            Zip_Code TEXT,
            Created_At TEXT,
            EDW_Load_Date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            Source_System TEXT DEFAULT 'OLTP_ECOMMERCE'
        )
    ''')
    
    # Product Table (3NF)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Product (
            Product_ID TEXT PRIMARY KEY,
            Product_Name TEXT,
            Category TEXT,
            Price REAL,
            Cost REAL,
            EDW_Load_Date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            Source_System TEXT DEFAULT 'OLTP_ECOMMERCE'
        )
    ''')
    
    # Order Header Table (3NF)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Order_Header (
            Order_ID TEXT PRIMARY KEY,
            Customer_ID TEXT,
            Order_Date TEXT,
            Status TEXT,
            EDW_Load_Date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            Source_System TEXT DEFAULT 'OLTP_ECOMMERCE',
            FOREIGN KEY (Customer_ID) REFERENCES Customer (Customer_ID)
        )
    ''')
    
    # Order Item Table (3NF)
    # Resolves Many-to-Many relationship between Orders and Products
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Order_Item (
            Order_Item_ID TEXT PRIMARY KEY,
            Order_ID TEXT,
            Product_ID TEXT,
            Quantity INTEGER,
            Unit_Price REAL,
            EDW_Load_Date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            Source_System TEXT DEFAULT 'OLTP_ECOMMERCE',
            FOREIGN KEY (Order_ID) REFERENCES Order_Header (Order_ID),
            FOREIGN KEY (Product_ID) REFERENCES Product (Product_ID)
        )
    ''')
    
    conn.commit()
    print("3NF Tables created successfully.")

def load_data(conn):
    # Load Customers
    print("Loading Customers...")
    df_customers = pd.read_csv(os.path.join(DATA_DIR, 'customers.csv'))
    # Rename columns to match DB schema if necessary, or just rely on order if simple
    # Here we map directly as names are similar
    df_customers.to_sql('Customer', conn, if_exists='append', index=False, method='multi', chunksize=100)
    
    # Load Products
    print("Loading Products...")
    df_products = pd.read_csv(os.path.join(DATA_DIR, 'products.csv'))
    df_products.to_sql('Product', conn, if_exists='append', index=False, method='multi', chunksize=100)
    
    # Load Orders
    print("Loading Orders...")
    df_orders = pd.read_csv(os.path.join(DATA_DIR, 'orders.csv'))
    # Rename columns to match our 3NF schema naming convention
    df_orders.rename(columns={
        'customer_id': 'Customer_ID',
        'order_id': 'Order_ID',
        'order_date': 'Order_Date',
        'status': 'Status'
    }, inplace=True)
    df_orders.to_sql('Order_Header', conn, if_exists='append', index=False, method='multi', chunksize=100)
    
    # Load Order Items
    print("Loading Order Items...")
    df_items = pd.read_csv(os.path.join(DATA_DIR, 'order_items.csv'))
    df_items.rename(columns={
        'order_item_id': 'Order_Item_ID',
        'order_id': 'Order_ID',
        'product_id': 'Product_ID',
        'quantity': 'Quantity',
        'unit_price': 'Unit_Price'
    }, inplace=True)
    df_items.to_sql('Order_Item', conn, if_exists='append', index=False, method='multi', chunksize=100)
    
    print("Data loaded successfully.")

def main():
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME) # Clean start for this demo
        
    conn = create_connection()
    if conn:
        create_tables(conn)
        load_data(conn)
        conn.close()
        print("Inmon ETL Process Complete.")

if __name__ == "__main__":
    main()

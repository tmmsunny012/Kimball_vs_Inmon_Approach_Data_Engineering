import pandas as pd
import sqlite3
import os
from datetime import datetime

# Configuration
DATA_DIR = 'data'
DB_NAME = 'kimball_dw.db'

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
    
    # Kimball Approach: Dimensional Modeling (Star Schema)
    # Focus on query performance, understandability, and business process
    
    # DimCustomer (Type 1 for simplicity, or Type 2 could be added)
    # Surrogate Key: Customer_Key
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS DimCustomer (
            Customer_Key INTEGER PRIMARY KEY AUTOINCREMENT,
            Customer_ID TEXT,
            Full_Name TEXT,
            Email TEXT,
            Location TEXT,
            Current_Flag TEXT DEFAULT 'Y',
            Effective_Date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            Expiration_Date TIMESTAMP DEFAULT '9999-12-31'
        )
    ''')
    
    # DimProduct
    # Surrogate Key: Product_Key
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS DimProduct (
            Product_Key INTEGER PRIMARY KEY AUTOINCREMENT,
            Product_ID TEXT,
            Product_Name TEXT,
            Category TEXT,
            Current_Price REAL
        )
    ''')
    
    # DimDate (Time Dimension - Critical for Kimball)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS DimDate (
            Date_Key INTEGER PRIMARY KEY,
            Full_Date DATE,
            Day_Name TEXT,
            Month_Name TEXT,
            Year INTEGER,
            Quarter INTEGER
        )
    ''')
    
    # FactSales
    # Granularity: One row per line item in an order
    # Foreign Keys to Dimensions
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS FactSales (
            Sales_Key INTEGER PRIMARY KEY AUTOINCREMENT,
            Date_Key INTEGER,
            Customer_Key INTEGER,
            Product_Key INTEGER,
            Order_ID TEXT,
            Quantity INTEGER,
            Unit_Price REAL,
            Total_Amount REAL,
            Cost_Amount REAL,
            Profit_Amount REAL,
            FOREIGN KEY (Date_Key) REFERENCES DimDate (Date_Key),
            FOREIGN KEY (Customer_Key) REFERENCES DimCustomer (Customer_Key),
            FOREIGN KEY (Product_Key) REFERENCES DimProduct (Product_Key)
        )
    ''')
    
    conn.commit()
    print("Star Schema Tables created successfully.")

def populate_dim_date(conn):
    # Generate a simple date dimension
    print("Populating DimDate...")
    start_date = datetime(2023, 1, 1)
    end_date = datetime.now()
    date_range = pd.date_range(start=start_date, end=end_date)
    
    dates = []
    for d in date_range:
        dates.append({
            'Date_Key': int(d.strftime('%Y%m%d')),
            'Full_Date': d.date(),
            'Day_Name': d.strftime('%A'),
            'Month_Name': d.strftime('%B'),
            'Year': d.year,
            'Quarter': d.quarter
        })
    
    df_dates = pd.DataFrame(dates)
    df_dates.to_sql('DimDate', conn, if_exists='replace', index=False)

def load_dimensions(conn):
    # Load DimCustomer
    print("Loading DimCustomer...")
    df_customers = pd.read_csv(os.path.join(DATA_DIR, 'customers.csv'))
    
    # Transformation: Combine First and Last Name
    df_customers['Full_Name'] = df_customers['first_name'] + ' ' + df_customers['last_name']
    
    # Transformation: Combine City and State
    df_customers['Location'] = df_customers['city'] + ', ' + df_customers['state']
    
    # Select only needed columns for Dimension
    dim_customer_data = df_customers[['customer_id', 'Full_Name', 'email', 'Location']].copy()
    dim_customer_data.rename(columns={'customer_id': 'Customer_ID', 'email': 'Email'}, inplace=True)
    
    dim_customer_data.to_sql('DimCustomer', conn, if_exists='append', index=False)
    
    # Load DimProduct
    print("Loading DimProduct...")
    df_products = pd.read_csv(os.path.join(DATA_DIR, 'products.csv'))
    dim_product_data = df_products[['product_id', 'product_name', 'category', 'price']].copy()
    dim_product_data.rename(columns={
        'product_id': 'Product_ID', 
        'product_name': 'Product_Name', 
        'category': 'Category', 
        'price': 'Current_Price'
    }, inplace=True)
    
    dim_product_data.to_sql('DimProduct', conn, if_exists='append', index=False)

def load_facts(conn):
    print("Loading FactSales...")
    
    # Read source data
    df_orders = pd.read_csv(os.path.join(DATA_DIR, 'orders.csv'))
    df_items = pd.read_csv(os.path.join(DATA_DIR, 'order_items.csv'))
    df_products = pd.read_csv(os.path.join(DATA_DIR, 'products.csv')) # Need cost for profit calc
    
    # Merge to get a flat view first
    df_merged = pd.merge(df_items, df_orders, on='order_id')
    df_merged = pd.merge(df_merged, df_products, on='product_id')
    
    # Read Dimensions to get Surrogate Keys
    dim_customer = pd.read_sql("SELECT Customer_Key, Customer_ID FROM DimCustomer", conn)
    dim_product = pd.read_sql("SELECT Product_Key, Product_ID FROM DimProduct", conn)
    
    # Map Surrogate Keys (Lookup)
    df_fact = pd.merge(df_merged, dim_customer, left_on='customer_id', right_on='Customer_ID', how='left')
    df_fact = pd.merge(df_fact, dim_product, left_on='product_id', right_on='Product_ID', how='left')
    
    # Create Date_Key
    df_fact['Date_Key'] = pd.to_datetime(df_fact['order_date']).dt.strftime('%Y%m%d').astype(int)
    
    # Calculate Measures
    df_fact['Total_Amount'] = df_fact['quantity'] * df_fact['unit_price']
    df_fact['Cost_Amount'] = df_fact['quantity'] * df_fact['cost']
    df_fact['Profit_Amount'] = df_fact['Total_Amount'] - df_fact['Cost_Amount']
    
    # Select columns for Fact Table
    fact_table = df_fact[[
        'Date_Key', 'Customer_Key', 'Product_Key', 'order_id', 
        'quantity', 'unit_price', 'Total_Amount', 'Cost_Amount', 'Profit_Amount'
    ]].copy()
    
    fact_table.rename(columns={
        'order_id': 'Order_ID',
        'quantity': 'Quantity',
        'unit_price': 'Unit_Price'
    }, inplace=True)
    
    fact_table.to_sql('FactSales', conn, if_exists='append', index=False)
    print("FactSales loaded successfully.")

def main():
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)
        
    conn = create_connection()
    if conn:
        create_tables(conn)
        populate_dim_date(conn)
        load_dimensions(conn)
        load_facts(conn)
        conn.close()
        print("Kimball ETL Process Complete.")

if __name__ == "__main__":
    main()

# Kimball vs. Inmon: A Practical Python Lesson

This lesson provides a hands-on comparison of the two dominant data warehousing methodologies: **Bill Inmon's Corporate Information Factory (CIF)** and **Ralph Kimball's Dimensional Modeling**.

We have built a mini-project that simulates an E-commerce environment to demonstrate these concepts.

## 1. The Scenario: E-Commerce Data

We started by generating synthetic data for an online store. This represents our **OLTP (Online Transaction Processing)** system.
*   **Goal**: Capture transactions quickly.
*   **Structure**: Highly normalized to avoid redundancy and ensure consistency.
*   **Files Generated**: `customers.csv`, `products.csv`, `orders.csv`, `order_items.csv`.

## 2. The Inmon Approach (The Factory)

**Philosophy**: "Single Version of the Truth"
Inmon believes in building a centralized, normalized **Enterprise Data Warehouse (EDW)** first. Data is integrated from all sources into a 3rd Normal Form (3NF) structure. Data Marts are then built *from* the EDW for specific reporting needs.

### Key Characteristics
*   **Top-Down**: Build the warehouse for the whole enterprise first.
*   **Normalized (3NF)**: Similar to OLTP but with history and integration.
*   **Neutral**: Data is stored neutrally, not biased towards any specific report.

### Our Implementation (`inmon_etl.py`)
We created `inmon_edw.db` with tables: `Customer`, `Product`, `Order_Header`, `Order_Item`.

**Code Highlight:**
Notice how we kept the tables separate and normalized.
```python
# From inmon_etl.py
cursor.execute('''
    CREATE TABLE IF NOT EXISTS Order_Item (
        Order_Item_ID TEXT PRIMARY KEY,
        Order_ID TEXT,
        Product_ID TEXT,
        ...
        FOREIGN KEY (Order_ID) REFERENCES Order_Header (Order_ID)
    )
''')
```

**Pros**:
*   Excellent for data consistency and integrity.
*   Flexible for unknown future requirements.
*   Easy to update data (less redundancy).

**Cons**:
*   Complex to query (requires many JOINs).
*   Takes longer to deliver value (high upfront effort).

## 3. The Kimball Approach (The Retailer)

**Philosophy**: "The Business Process"
Kimball believes in building **Dimensional Data Marts** that directly answer business questions. The Data Warehouse is essentially the union of all these Data Marts (conformed dimensions).

### Key Characteristics
*   **Bottom-Up**: Build Data Marts for specific business processes (e.g., Sales, Inventory) one by one.
*   **Denormalized (Star Schema)**: Optimized for reading and reporting.
*   **User-Centric**: Structured the way users think about the business.

### Our Implementation (`kimball_etl.py`)
We created `kimball_dw.db` with a **Star Schema**:
*   **Fact Table**: `FactSales` (The measurements: Quantity, Amount, Profit).
*   **Dimension Tables**: `DimCustomer`, `DimProduct`, `DimDate` (The context: Who, What, When).

**Code Highlight:**
Notice how we pre-calculated values and "flattened" the data.
```python
# From kimball_etl.py
# We merge everything into a single wide view before loading the Fact table
df_merged = pd.merge(df_items, df_orders, on='order_id')
df_merged = pd.merge(df_merged, df_products, on='product_id')

# We calculate profit during ETL, not at query time
df_fact['Profit_Amount'] = df_fact['Total_Amount'] - df_fact['Cost_Amount']
```

**Pros**:
*   Fast query performance (fewer JOINs).
*   Easy for business users to understand.
*   Quick to deliver value (iterative).

**Cons**:
*   Data redundancy (denormalized).
*   Harder to maintain consistency if dimensions change (e.g., Customer changes address).

## 4. Comparison: Querying the Data

Let's see the difference when we want to answer a simple question: **"What is the total sales amount by Customer City?"**

### Inmon Query (Many JOINs)
```sql
SELECT 
    c.City, 
    SUM(oi.Quantity * oi.Unit_Price) as Total_Sales
FROM 
    Order_Item oi
    JOIN Order_Header oh ON oi.Order_ID = oh.Order_ID
    JOIN Customer c ON oh.Customer_ID = c.Customer_ID
GROUP BY 
    c.City;
```

### Kimball Query (Fewer JOINs, Simple Star Join)
```sql
SELECT 
    c.Location, -- We already combined City/State in ETL
    SUM(f.Total_Amount) as Total_Sales -- Pre-calculated
FROM 
    FactSales f
    JOIN DimCustomer c ON f.Customer_Key = c.Customer_Key
GROUP BY 
    c.Location;
```

## Summary Table

| Feature | Inmon (CIF) | Kimball (Dimensional) |
| :--- | :--- | :--- |
| **Primary Goal** | Enterprise-wide consistency | Query performance & Usability |
| **Data Model** | Normalized (3NF) | Denormalized (Star Schema) |
| **Approach** | Top-Down (EDW first) | Bottom-Up (Data Marts first) |
| **Complexity** | High (ETL is simple, Query is hard) | Low (ETL is complex, Query is simple) |
| **Time to Value** | Slow | Fast |

## How to Run the Examples

1.  **Generate Data**:
    ```bash
    python generate_data.py
    ```
2.  **Run Inmon ETL**:
    ```bash
    python inmon_etl.py
    ```
    *Check `inmon_edw.db` using a SQLite viewer.*
3.  **Run Kimball ETL**:
    ```bash
    python kimball_etl.py
    ```
    *Check `kimball_dw.db` using a SQLite viewer.*

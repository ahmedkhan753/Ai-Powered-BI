# We Are going to build the Gold layer which will take the data from silver layer and then distribute it into facts and dimensions for performance, BI dashboards and queries

# 1. Import necessary libraries
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
import os
import uuid
import datetime
from load_from_silver import load_from_silver

# Define database connection parameters
gold_db_host = os.getenv("GOLD_DB_HOST", "localhost")
gold_db_port = os.getenv("GOLD_DB_PORT", "5434")
gold_db_url = f"postgresql://admin:password123@{gold_db_host}:{gold_db_port}/bi_warehouse_warehouse"
engine_gold = create_engine(gold_db_url)

# 5. Define the star schema
class StarSchema:
    def __init__(self, engine, silver_data):
        self.engine = engine
        self.silver_data = silver_data

    def build_star_schema(self):
        # Build the star schema
        print("Building Dimension Tables...")
        self.build_dimension_tables()
        print("Building Fact Table...")
        self.build_fact_table()

    def build_dimension_tables(self):
        # --- Dim Date ---
        print("Building dim_date...")
        # Get unique dates
        unique_dates = pd.to_datetime(self.silver_data['sale_date'].unique())
        dim_date = pd.DataFrame({'full_date': unique_dates})
        
        # Generate date attributes
        dim_date['date_key'] = dim_date['full_date'].dt.strftime('%Y%m%d').astype(int)
        dim_date['year'] = dim_date['full_date'].dt.year
        dim_date['month'] = dim_date['full_date'].dt.month
        dim_date['day'] = dim_date['full_date'].dt.day
        dim_date['quarter'] = dim_date['full_date'].dt.quarter
        dim_date['weekday_name'] = dim_date['full_date'].dt.day_name()
        dim_date['is_weekend'] = dim_date['full_date'].dt.weekday >= 5
        
        # Load into database
        dim_date.to_sql('dim_date', self.engine, schema='warehouse', if_exists='append', index=False, method='multi')

        # --- Dim Product ---
        print("Building dim_product...")
        # Get unique products
        unique_products = self.silver_data[['product']].drop_duplicates()
        # Map 'product' to 'product_name' and set a default 'category'
        unique_products = unique_products.rename(columns={'product': 'product_name'})
        unique_products['category'] = 'General' # Default category as we only have product name
        
        # Add attributes
        unique_products['start_date'] = datetime.date.today()
        unique_products['end_date'] = None
        unique_products['is_current'] = True
        
        # We need a primary key 'product_key'.
        unique_products['product_key'] = range(1, len(unique_products) + 1)
        
        unique_products.to_sql('dim_product', self.engine, schema='warehouse', if_exists='append', index=False, method='multi')


        # --- Dim Customer ---
        print("Building dim_customer...")
        # Get unique customers
        # Based on ERD: customer_key, gender, age, age_group, spend_category, start_date, end_date, is_current
        # Source (Silver): gender, customer_age, spend_category. 
        customer_cols = ['gender', 'customer_age', 'spend_category']
        dim_customer = self.silver_data[customer_cols].drop_duplicates()
        
        # Generate attributes
        dim_customer = dim_customer.rename(columns={'customer_age': 'age'})
        
        # Age Group Logic
        conditions = [
            (dim_customer['age'] < 20),
            (dim_customer['age'] >= 20) & (dim_customer['age'] < 30),
            (dim_customer['age'] >= 30) & (dim_customer['age'] < 40),
            (dim_customer['age'] >= 40) & (dim_customer['age'] < 50),
            (dim_customer['age'] >= 50) & (dim_customer['age'] < 60),
            (dim_customer['age'] >= 60)
        ]
        choices = ['Teen', '20s', '30s', '40s', '50s', '60+']
        dim_customer['age_group'] = np.select(conditions, choices, default='Unknown')

        dim_customer['start_date'] = datetime.date.today()
        dim_customer['end_date'] = None
        dim_customer['is_current'] = True
        
        # Generate surrogate key
        dim_customer['customer_key'] = range(1, len(dim_customer) + 1)
        
        dim_customer.to_sql('dim_customer', self.engine, schema='warehouse', if_exists='append', index=False, method='multi')
        
    def build_fact_table(self):
        # Merge source data with dimensions to get keys
        
        # 1. Get Date Key
        self.silver_data['date_key'] = pd.to_datetime(self.silver_data['sale_date']).dt.strftime('%Y%m%d').astype(int)
        
        # 2. Get Product Key
        dim_product = pd.read_sql("SELECT product_key, product_name FROM warehouse.dim_product", self.engine)
        # Match on product_name since that's what we have
        fact_merged = self.silver_data.merge(dim_product, left_on='product', right_on='product_name', how='left')
        
        # 3. Get Customer Key
        dim_customer = pd.read_sql("SELECT customer_key, gender, age, spend_category FROM warehouse.dim_customer", self.engine)
        # Join on all defining attributes
        fact_merged = fact_merged.merge(dim_customer, left_on=['gender', 'customer_age', 'spend_category'], right_on=['gender', 'age', 'spend_category'], how='left')
        
        # 4. Prepare Fact Table
        
        fact_sales = pd.DataFrame()
        # Generate Integer Order ID
        # Since we don't have real order IDs, we'll use a sequence starting from 1000
        fact_sales['order_id'] = range(1000, 1000 + len(fact_merged))
        fact_sales['date_key'] = fact_merged['date_key']
        fact_sales['customer_key'] = fact_merged['customer_key']
        fact_sales['product_key'] = fact_merged['product_key']
        fact_sales['quantity'] = fact_merged['quantity']
        fact_sales['price'] = fact_merged['price']
        fact_sales['sales_amount'] = fact_merged['sales_amount']
        fact_sales['ingestion_timestamp'] = datetime.datetime.now()
        
        # sales_key is likely an auto-increment PK in the DB.
        
        fact_sales.to_sql('fact_sales', self.engine, schema='warehouse', if_exists='append', index=False, method='multi')


if __name__ == "__main__":
    # Load data
    print("Loading silver data...")
    silver_data_df = load_from_silver()
    
    # Initialize and run Star Schema build
    star_schema = StarSchema(engine_gold, silver_data_df)
    star_schema.build_star_schema()
    
    # Close connection
    engine_gold.dispose()
    print("Star schema built successfully!")

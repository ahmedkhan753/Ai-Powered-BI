# We Are going to build the Gold layer which will take the data from silver layer and then distribute it into facts and dimensions for performance, BI dashboards and queries

# 1. Import necessary libraries
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
import os
from load_from_silver import load_from_silver

gold_db_host = os.getenv("GOLD_DB_HOST", "localhost")
gold_db_port = os.getenv("GOLD_DB_PORT", "5434")
gold_db_url = f"postgresql://admin:password123@{gold_db_host}:{gold_db_port}/bi_warehouse_warehouse"
engine_gold = create_engine(gold_db_url)

silver_data = load_from_silver()

# 5. Define the star schema
class StarSchema:
    def __init__(self, engine, silver_data):
        self.engine = engine
        self.silver_data = silver_data

    def build_star_schema(self):
        # Build the star schema
        self.build_fact_table()
        self.build_dimension_tables()

    def build_fact_table(self):
        # Here we will populate the fact table
        gold_fact_sales = pd.read_sql("INSERT INTO warehouse.fact_sales(sales_key,order_id,date_key,customer_key,product_key,quantity,price,sales_amount,ingestion_timestamp)",self.engine_gold,con=)


    def build_dimension_tables(self):
        # Build the dimension tables
        pass

# 6. Create an instance of the star schema
star_schema = StarSchema(engine_gold)

# 7. Build the star schema
star_schema.build_star_schema()

# 8. Close the database connection
engine_gold.dispose()

# 9. Print a success message
print("Star schema built successfully!")

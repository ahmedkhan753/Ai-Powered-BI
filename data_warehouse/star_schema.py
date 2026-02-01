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
    def __init__(self, engine):
        self.engine = engine

    def build_star_schema(self):
        # Build the star schema
        self.build_fact_table()
        self.build_dimension_tables()

    def build_fact_table(self):
        # Build the fact table
        pass

    def build_dimension_tables(self):
        # Build the dimension tables
        pass

# 6. Create an instance of the star schema
star_schema = StarSchema(engine)

# 7. Build the star schema
star_schema.build_star_schema()

# 8. Close the database connection
engine.dispose()

# 9. Print a success message
print("Star schema built successfully!")

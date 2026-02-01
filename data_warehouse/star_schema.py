# We Are going to build the Gold layer which will take the data from silver layer and then distribute it into facts and dimensions for performance, BI dashboards and queries


# 1. Import necessary libraries
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# 3. Define database connection parameters
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# 4. Create database connection
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

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

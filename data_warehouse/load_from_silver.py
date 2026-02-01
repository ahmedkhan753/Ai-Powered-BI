# We are going to load the data from the silver layer to the gold layer

# 1. Import necessary libraries
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
import os

clean_db_host = os.getenv("CLEAN_DB_HOST", "localhost")
clean_db_port = os.getenv("CLEAN_DB_PORT", "5434")
clean_db_url = f"postgresql://admin:password123@{clean_db_host}:{clean_db_port}/bi_warehouse_clean"
engine_clean = create_engine(clean_db_url)

# 5. Define the function to load data from the silver layer
def load_from_silver():
    """Load data from the silver layer (clean schema) into a DataFrame."""
    # Use the clean schema explicitly if needed, although read_sql handles the query
    clean_data = pd.read_sql("SELECT * FROM clean.clean_sales_data", engine_clean)
    return clean_data

if __name__ == "__main__":
    # 6. Load the data
    print("Loading data from silver layer...")
    df_silver = load_from_silver()
    
    # 7. Print a success message and preview
    print("Data loaded from silver layer successfully!")
    print(df_silver.head())
    
    # 8. Close the database connection
    engine_clean.dispose()
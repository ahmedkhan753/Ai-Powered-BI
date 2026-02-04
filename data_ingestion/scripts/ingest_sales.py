
import logging
import pandas as pd
from sqlalchemy import create_engine
from extract import extract_data
from validate import validate_data
from load_raw import load_raw_sales

import os

db_user = os.getenv("POSTGRES_USER", "admin")
db_password = os.getenv("POSTGRES_PASSWORD", "password123")
raw_host = os.getenv("RAW_DB_HOST", "postgres")
raw_port = os.getenv("RAW_DB_PORT", "5432")
raw_db = os.getenv("POSTGRES_DB", "bi_warehouse")

DATABASE_URL = f"postgresql://{db_user}:{db_password}@{raw_host}:{raw_port}/{raw_db}"
def get_max_loaded_order_id() -> int:
    """Query DB for highest order_id already loaded. Return -1 if table empty."""
    engine = create_engine(DATABASE_URL)
    try:
        result = pd.read_sql("SELECT MAX(order_id) FROM raw.sales_data", engine)
        max_id = result.iloc[0, 0]
        return -1 if max_id is None else int(max_id)
    except Exception:
        return -1 
    finally:
        engine.dispose()

def ingest_sales_pipeline():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    logging.info("Starting automated incremental ingestion pipeline")
    
    # 1. Extract full dataset
    full_df = extract_data("source/Sales_Dataset.csv")
    
    # 2. Get current watermark
    max_loaded = get_max_loaded_order_id()
    logging.info(f"Current max order_id in DB: {max_loaded}")
    
    # 3. Filter only new rows
    potential_new_df = full_df[full_df['OrderID'] > max_loaded]
    new_df = potential_new_df.head(100)
    
    if new_df.empty:
        logging.info("No new rows to load — pipeline complete!")
        return
    
    # Schema Validation To avoid pipeline failure due to data schema mismatch 
    expected_columns = ['OrderID', 'Date', 'Gender', 'Age', 'Product', 'Quantity', 'Price', 'Total Amount']
    if not all(col in df.columns for col in expected_columns):
        logging.error("Data schema mismatch - expected columns not found.")
        return -1 
    
    logging.info(f"Found {len(new_df)} new rows (OrderID {new_df['OrderID'].min()} to {new_df['OrderID'].max()})")
    
    # 4. Validate and load
    validate_data(new_df)
    max_this_run = load_raw_sales(new_df)
    
    logging.info(f"Pipeline completed! New max order_id: {max_this_run}")

if __name__ == "__main__":
    ingest_sales_pipeline()
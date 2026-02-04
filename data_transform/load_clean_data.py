# Going to load data from bronze layer to silver layer
import pandas as pd
from data_transform.clean_sales import CleanSales
from data_transform.raw_data_fetcher import engine, query, raw_data_fetcher
from sqlalchemy import create_engine
import os

clean_db_host = os.getenv("CLEAN_DB_HOST", "localhost")
clean_db_port = os.getenv("CLEAN_DB_PORT", "5434")
db_user = os.getenv("POSTGRES_USER", "admin")
db_password = os.getenv("POSTGRES_PASSWORD", "password123")
clean_db = os.getenv("POSTGRES_DB_CLEAN", "bi_warehouse_clean")

clean_db_url = f"postgresql://{db_user}:{db_password}@{clean_db_host}:{clean_db_port}/{clean_db}"
engine_clean = create_engine(clean_db_url)
def extract_clean_data() -> pd.DataFrame:
    # 1. Get current watermark from silver layer
    try:
        max_id_result = pd.read_sql("SELECT MAX(order_id) FROM clean.clean_sales_data", engine_clean)
        max_id = max_id_result.iloc[0, 0]
        max_id = -1 if max_id is None else int(max_id)
    except Exception:
        max_id = -1
    
    # 2. Extract only new data from raw layer
    incremental_query = f"SELECT * FROM raw.sales_data WHERE order_id > {max_id}"
    raw_data = pd.read_sql(incremental_query, engine_clean.connect()) # using engine_clean to connect to the DB might be okay if they are in the same DB or accessible
    
    if raw_data.empty:
        return pd.DataFrame()

    cleaner = CleanSales(raw_data)
    cleaned_data = cleaner.clean_data()
    return cleaned_data

def load_clean_data(clean_data):
    if clean_data.empty:
        print("No new data to load to silver layer.")
        return

    clean_data.to_sql(
        name="clean_sales_data",
        schema="clean",
        con=engine_clean,
        if_exists="append",
        index=False,
        method="multi"
    )

if __name__ == "__main__":
    clean_data = extract_clean_data()
    load_clean_data(clean_data)
    print("Clean data loaded successfully!")
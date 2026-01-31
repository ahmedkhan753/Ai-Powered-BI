# Going to load data from bronze layer to silver layer
import pandas as pd
from data_transform.clean_sales import CleanSales
from data_transform.raw_data_fetcher import engine, query, raw_data_fetcher
from sqlalchemy import create_engine
import os

clean_db_host = os.getenv("CLEAN_DB_HOST", "localhost")
clean_db_port = os.getenv("CLEAN_DB_PORT", "5434")
clean_db_url = f"postgresql://admin:password123@{clean_db_host}:{clean_db_port}/bi_warehouse_clean"
engine_clean = create_engine(clean_db_url)
def extract_clean_data() -> pd.DataFrame:
    raw_data = raw_data_fetcher(query, engine)
    cleaner = CleanSales(raw_data)
    cleaned_data = cleaner.clean_data()
    return cleaned_data

def load_clean_data(clean_data):
    clean_data.to_sql(
        name="clean_sales_data",
        schema="clean",
        con=engine_clean,
        if_exists="replace",
        index=False,
        method="multi"
    )

if __name__ == "__main__":
    clean_data = extract_clean_data()
    load_clean_data(clean_data)
    print("Clean data loaded successfully!")
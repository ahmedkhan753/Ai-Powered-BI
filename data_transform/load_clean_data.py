# Going to load data from bronze layer to silver layer
import pandas as pd
from clean_sales import CleanSales
from raw_data_fetcher import engine, query, raw_data_fetcher
from sqlalchemy import create_engine

engine_clean = create_engine("postgresql://admin:password123@localhost:5434/bi_warehouse_clean")

clean_db_url = "postgresql://admin:password123@localhost:5434/bi_warehouse_clean"
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
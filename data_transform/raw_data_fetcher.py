import logging
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

db_host = os.getenv("RAW_DB_HOST", "localhost")
db_port = os.getenv("RAW_DB_PORT", "5433")
database_url = f"postgresql://admin:password123@{db_host}:{db_port}/bi_warehouse"
engine = create_engine(database_url)
query = "SELECT * FROM raw.sales_data;"

def raw_data_fetcher(sql_query, engine) -> pd.DataFrame:
    df = pd.read_sql(sql_query, engine)
    logging.info(f"Fetched {len(df)} records from raw.sales_data")
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    data = raw_data_fetcher(query, engine)
    print(data.head())
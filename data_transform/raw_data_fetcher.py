import logging
import os
import pandas as pd
from sqlalchemy import create_engine

db_host = os.getenv("RAW_DB_HOST", "localhost")
db_port = os.getenv("RAW_DB_PORT", "5433")
db_user = os.getenv("POSTGRES_USER", "admin")
db_password = os.getenv("POSTGRES_PASSWORD", "password123")
db_name = os.getenv("POSTGRES_DB", "bi_warehouse")

database_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
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
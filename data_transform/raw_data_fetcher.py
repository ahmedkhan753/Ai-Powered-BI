import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError

database_url = "postgresql://admin:password123@localhost:5433/bi_warehouse_raw"
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
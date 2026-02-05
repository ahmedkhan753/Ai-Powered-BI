import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError


import os

db_user = os.getenv("POSTGRES_USER", "admin")
db_password = os.getenv("POSTGRES_PASSWORD", "password123")
raw_host = os.getenv("RAW_DB_HOST", "postgres")
raw_port = os.getenv("RAW_DB_PORT", "5432")
raw_db = os.getenv("POSTGRES_DB", "bi_warehouse")

DATABASE_URL = (
    f"postgresql://{db_user}:{db_password}@{raw_host}:{raw_port}/{raw_db}"  # Raw DB URL
)


def load_raw_sales(df: pd.DataFrame) -> int:
    """
    Loads new rows and returns the max order_id loaded in this batch.
    """
    if df.empty:
        logging.info("No new data to load.")
        return -1

    logging.info(f"Starting load of {len(df)} new rows")

    df_renamed = df.rename(
        columns={
            "OrderID": "order_id",
            "Date": "date",
            "Gender": "gender",
            "Age": "age",
            "Product": "product",
            "Quantity": "quantity",
            "Price": "price",
            "Total Amount": "total_amount",
        }
    ).copy()

    engine = create_engine(DATABASE_URL)

    try:
        df_renamed.to_sql(
            name="sales_data",
            schema="raw",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )
        logging.info("Load completed successfully!")
        return df_renamed["order_id"].max()
    except Exception as e:
        logging.error(f"Error during load: {e}")
        raise
    finally:
        engine.dispose()

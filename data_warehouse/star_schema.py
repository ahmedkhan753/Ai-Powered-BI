# We Are going to build the Gold layer which will take the data from silver layer and then distribute it into facts and dimensions for performance, BI dashboards and queries

# 1. Import necessary libraries
import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
import os
import uuid
import datetime
from data_warehouse.load_from_silver import load_from_silver

# Define database connection parameters inside the function/class scope to avoid side effects on import.


# 5. Define the star schema
class StarSchema:
    def __init__(self, engine, silver_data):
        self.engine = engine
        self.silver_data = silver_data

    def build_star_schema(self):
        # Ensure schema exists
        with self.engine.begin() as conn:
            conn.execute(sqlalchemy.text("CREATE SCHEMA IF NOT EXISTS warehouse;"))

        # Build the star schema
        print("Building Dimension Tables...")
        self.build_dimension_tables()
        print("Building Fact Table...")
        self.build_fact_table()

    def get_existing_data(self, query, columns):
        """Helper to read existing data, returning empty DF with columns if table doesn't exist."""
        try:
            df = pd.read_sql(query, self.engine)
            if df.empty:
                return pd.DataFrame(columns=columns)
            return df
        except Exception:
            return pd.DataFrame(columns=columns)

    def build_dimension_tables(self):
        # --- Dim Date ---
        print("Building dim_date...")
        # Get unique dates
        unique_dates = pd.to_datetime(self.silver_data["sale_date"].unique())
        dim_date = pd.DataFrame({"full_date": unique_dates})

        # Generate date attributes
        dim_date["date_key"] = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
        dim_date["year"] = dim_date["full_date"].dt.year
        dim_date["month"] = dim_date["full_date"].dt.month
        dim_date["day"] = dim_date["full_date"].dt.day
        dim_date["quarter"] = dim_date["full_date"].dt.quarter
        dim_date["weekday_name"] = dim_date["full_date"].dt.day_name()
        dim_date["is_weekend"] = dim_date["full_date"].dt.weekday >= 5

        # Check existing dates using helper
        existing_dates = self.get_existing_data(
            "SELECT date_key FROM warehouse.dim_date", ["date_key"]
        )
        if not existing_dates.empty:
            dim_date = dim_date[~dim_date["date_key"].isin(existing_dates["date_key"])]

        if not dim_date.empty:
            print(f"Inserting {len(dim_date)} new rows into dim_date")
            dim_date.to_sql(
                "dim_date",
                self.engine,
                schema="warehouse",
                if_exists="append",
                index=False,
                method="multi",
            )
        else:
            print("dim_date is up to date.")

        # --- Dim Product ---
        print("Building dim_product...")
        # Get unique products
        # Mapping 'product' from silver to 'category' in dim_product based on instructions
        unique_products = self.silver_data[["product"]].drop_duplicates()
        unique_products = unique_products.rename(columns={"product": "category"})

        # Add attributes
        unique_products["start_date"] = datetime.date.today()
        unique_products["end_date"] = None
        unique_products["is_current"] = True

        # Check existing products (by category)
        existing_products = self.get_existing_data(
            "SELECT category FROM warehouse.dim_product", ["category"]
        )
        if not existing_products.empty:
            unique_products = unique_products[
                ~unique_products["category"].isin(existing_products["category"])
            ]

        if not unique_products.empty:
            print(f"Inserting {len(unique_products)} new rows into dim_product")
            unique_products.to_sql(
                "dim_product",
                self.engine,
                schema="warehouse",
                if_exists="append",
                index=False,
                method="multi",
            )
        else:
            print("dim_product is up to date.")

        # --- Dim Customer ---
        print("Building dim_customer...")
        # Get unique customers
        # Source (Silver): gender, customer_age, spend_category.
        customer_cols = ["gender", "customer_age", "spend_category"]
        dim_customer = self.silver_data[customer_cols].drop_duplicates()

        # Generate attributes
        dim_customer = dim_customer.rename(columns={"customer_age": "age"})

        # Age Group Logic
        conditions = [
            (dim_customer["age"] < 20),
            (dim_customer["age"] >= 20) & (dim_customer["age"] < 30),
            (dim_customer["age"] >= 30) & (dim_customer["age"] < 40),
            (dim_customer["age"] >= 40) & (dim_customer["age"] < 50),
            (dim_customer["age"] >= 50) & (dim_customer["age"] < 60),
            (dim_customer["age"] >= 60),
        ]
        choices = ["Teen", "20s", "30s", "40s", "50s", "60+"]
        dim_customer["age_group"] = np.select(conditions, choices, default="Unknown")

        dim_customer["start_date"] = datetime.date.today()
        dim_customer["end_date"] = None
        dim_customer["is_current"] = True

        # Check existing customers
        # We can join on 'gender', 'age', 'spend_category' to filter
        existing_customers = self.get_existing_data(
            "SELECT gender, age, spend_category FROM warehouse.dim_customer",
            ["gender", "age", "spend_category"],
        )

        if not existing_customers.empty:
            # Create a composite key for easier filtering
            dim_customer["_key"] = (
                dim_customer["gender"].astype(str)
                + "_"
                + dim_customer["age"].astype(str)
                + "_"
                + dim_customer["spend_category"].astype(str)
            )
            existing_customers["_key"] = (
                existing_customers["gender"].astype(str)
                + "_"
                + existing_customers["age"].astype(str)
                + "_"
                + existing_customers["spend_category"].astype(str)
            )

            dim_customer = dim_customer[
                ~dim_customer["_key"].isin(existing_customers["_key"])
            ]
            dim_customer = dim_customer.drop(columns=["_key"])

        # Rely on DB SERIAL for customer_key

        if not dim_customer.empty:
            print(f"Inserting {len(dim_customer)} new rows into dim_customer")
            dim_customer.to_sql(
                "dim_customer",
                self.engine,
                schema="warehouse",
                if_exists="append",
                index=False,
                method="multi",
            )
        else:
            print("dim_customer is up to date.")

    def build_fact_table(self):
        # Merge source data with dimensions to get keys

        # 1. Get Date Key
        self.silver_data["date_key"] = (
            pd.to_datetime(self.silver_data["sale_date"])
            .dt.strftime("%Y%m%d")
            .astype(int)
        )

        # 2. Get Product Key
        # Fetch fresh keys from DB (inc. newly inserted ones)
        dim_product = self.get_existing_data(
            "SELECT product_key, category FROM warehouse.dim_product",
            ["product_key", "category"],
        )
        if dim_product.empty:
            print("Warning: dim_product is empty, fact merge will result in NaN keys")
        fact_merged = self.silver_data.merge(
            dim_product, left_on="product", right_on="category", how="left"
        )

        # 3. Get Customer Key
        dim_customer = self.get_existing_data(
            "SELECT customer_key, gender, age, spend_category FROM warehouse.dim_customer",
            ["customer_key", "gender", "age", "spend_category"],
        )
        if dim_customer.empty:
            print("Warning: dim_customer is empty, fact merge will result in NaN keys")
        fact_merged = fact_merged.merge(
            dim_customer,
            left_on=["gender", "customer_age", "spend_category"],
            right_on=["gender", "age", "spend_category"],
            how="left",
        )

        # 4. Prepare Fact Table

        fact_sales = pd.DataFrame()
        # Use order_id from the source data (Silver layer)
        fact_sales["order_id"] = fact_merged["order_id"]
        fact_sales["date_key"] = fact_merged["date_key"]
        fact_sales["customer_key"] = fact_merged["customer_key"]
        fact_sales["product_key"] = fact_merged["product_key"]
        fact_sales["quantity"] = fact_merged["quantity"]
        fact_sales["price"] = fact_merged["price"]
        fact_sales["sales_amount"] = fact_merged["sales_amount"]
        # Use the ingestion_timestamp from the silver layer to maintain consistency
        fact_sales["ingestion_timestamp"] = fact_merged["ingestion_timestamp"]

        # sales_key is likely an auto-increment PK in the DB.

        if not fact_sales.empty:
            print(f"Inserting {len(fact_sales)} new rows into fact_sales")
            fact_sales.to_sql(
                "fact_sales",
                self.engine,
                schema="warehouse",
                if_exists="append",
                index=False,
                method="multi",
            )
        else:
            print("No new facts to add.")


def run_star_schema_etl():
    # Define database connection parameters
    gold_db_host = os.getenv("GOLD_DB_HOST", "clean-warehouse-postgres")
    gold_db_port = os.getenv("GOLD_DB_PORT", "5432")
    db_user = os.getenv("POSTGRES_USER", "admin")
    db_password = os.getenv("POSTGRES_PASSWORD", "password123")

    # Use a dedicated variable for the target gold database
    gold_db_name = os.getenv("POSTGRES_DB_GOLD", "bi_warehouse_warehouse")

    gold_db_url = f"postgresql://{db_user}:{db_password}@{gold_db_host}:{gold_db_port}/{gold_db_name}"
    engine_gold = create_engine(gold_db_url)

    try:
        # 1. Get the last ingestion time from gold layer
        last_ingestion_time = None
        try:
            with engine_gold.connect() as conn:
                result = conn.execute(
                    sqlalchemy.text(
                        "SELECT MAX(ingestion_timestamp) FROM warehouse.fact_sales"
                    )
                )
                last_ingestion_time = result.scalar()
        except Exception as e:
            print(f"Could not fetch last_ingestion_time (might be first run): {e}")

        # 2. Load data from silver layer incrementally
        print(f"Loading silver data after {last_ingestion_time}...")
        silver_data_df = load_from_silver(last_ingestion_time)
        print(f"Extracted {len(silver_data_df)} new rows from silver layer.")

        if silver_data_df.empty:
            print("No new data to process.")
            return

        # 3. Initialize and run Star Schema build
        star_schema = StarSchema(engine_gold, silver_data_df)
        star_schema.build_star_schema()
        print("Star schema updated successfully!")
    finally:
        # Close connection
        engine_gold.dispose()


if __name__ == "__main__":
    run_star_schema_etl()

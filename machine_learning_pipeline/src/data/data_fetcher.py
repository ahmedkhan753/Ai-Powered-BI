import pandas as pd
from src.utils.logger import get_logger
import os
from sqlalchemy import create_engine

logger = get_logger(__name__)
 # Define database connection parameters
gold_db_host = os.getenv("GOLD_DB_HOST", "clean-warehouse-postgres")
gold_db_port = os.getenv("GOLD_DB_PORT", "5432")
db_user = os.getenv("POSTGRES_USER", "admin")
db_password = os.getenv("POSTGRES_PASSWORD", "password123")
 # Use a dedicated variable for the target gold database
gold_db = os.getenv("POSTGRES_DB_GOLD", "bi_warehouse_warehouse")

gold_db_url = f"postgresql://{db_user}:{db_password}@{gold_db_host}:{gold_db_port}/{gold_db}"
engine_gold = create_engine(gold_db_url)

def fetch_data(config):
    """
    Fetches data from the Gold Layer (Database) to simulate production environment.
    """
    try:
        logger.info("Fetching data from Gold Layer...")
        
        return df
        
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        raise e

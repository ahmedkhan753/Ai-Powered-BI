import sys
import os
import pandas as pd
import numpy as np

# Add the project root to sys.path to allow importing from src
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../.."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.utils.logger import get_logger
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError


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
        query = config['database']['query']
        df = pd.read_sql(query, engine_gold)
        return df
        
    except Exception as e:
        logger.warning(f"Connection failed to {gold_db_host}:{gold_db_port}. Error: {e}")
        
        # If we failed to connect to 'clean-warehouse-postgres' but we are on the host, 
        # we might want to try localhost:5434 as a second automatic attempt before mock data.
        if gold_db_host == "clean-warehouse-postgres":
            try:
                logger.info("Host 'clean-warehouse-postgres' not found. Attempting connection via localhost:5434...")
                fallback_url = f"postgresql://{db_user}:{db_password}@localhost:5434/{gold_db}"
                engine_local = create_engine(fallback_url)
                df = pd.read_sql(query, engine_local)
                logger.info("Successfully connected to database via localhost:5434.")
                return df
            except Exception as local_e:
                logger.warning(f"Connection to localhost:5434 also failed: {local_e}")

        logger.info("Generating mock data for verification...")
        
        # Generate 1000 rows of mock data
        num_rows = 1000
        dates = pd.date_range(start='2024-01-01', periods=100, freq='D')
        categories = ['Electronics', 'Clothing', 'Home', 'Toys']
        
        mock_data = []
        for i in range(num_rows):
            date = pd.Timestamp(np.random.choice(dates))
            mock_data.append({
                'sales_amount': np.random.uniform(20, 500),
                'quantity': np.random.randint(1, 10),
                'product_key': np.random.randint(1, 20),
                'product_category': np.random.choice(categories),
                'year': date.year,
                'month': date.month,
                'day': date.day,
                'quarter': (date.month - 1) // 3 + 1,
                'weekday_name': date.day_name(),
                'is_weekend': date.weekday() >= 5
            })
        
        df = pd.DataFrame(mock_data)
        logger.info("Successfully generated mock data.")
        return df

if __name__ == "__main__":
    import numpy as np # Ensure numpy is available for mock data
    config = {
        "database": {
            "query": "SELECT * FROM warehouse.fact_sales;"
        }
    }
    data = fetch_data(config)
    print(data.head())


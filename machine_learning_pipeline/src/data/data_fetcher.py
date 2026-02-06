import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)

def fetch_data(config):
    """
    Fetches data from the Gold Layer (Database) to simulate production environment.
    """
    try:
        logger.info("Fetching data from Gold Layer...")
        
        # In a real scenario, use SQLAlchemy:
        # engine = create_engine(config['database']['connection_string'])
        # df = pd.read_sql(config['database']['query'], engine)
        
        # For now, simulate with dummy data or load from local CSV if exists
        # This ensures the user can run it immediately without a live DB
        data = {
            'feature1': range(100),
            'feature2': [x * 2 for x in range(100)],
            'price': [100 + x * 5 + (x%10) for x in range(100)]
        }
        df = pd.DataFrame(data)
        
        logger.info(f"Data fetched successfully. Metadata: {df.shape}")
        return df
        
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        raise e

import yaml
import sys
from src.data.data_fetcher import fetch_data
from src.preprocess.preprocessor import preprocess_data
from src.models.train import train_model
from src.utils.logger import get_logger

logger = get_logger(__name__)

def load_config(config_path="config/config.yaml"):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def main():
    try:
        logger.info("Pipeline Execution Started")
        
        # 1. Load Config
        config = load_config()
        
        # 2. Fetch Data
        df = fetch_data(config)
        
        # 3. Preprocess
        X_train, X_test, y_train, y_test = preprocess_data(df, config)
        
        # 4. Train & Evaluate
        score = train_model(X_train, X_test, y_train, y_test, config)
        
        logger.info("Pipeline Execution Completed Successfully")
        print(f"Pipeline Finished. Final Model Score (R2): {score}")
        
    except Exception as e:
        logger.error(f"Pipeline Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

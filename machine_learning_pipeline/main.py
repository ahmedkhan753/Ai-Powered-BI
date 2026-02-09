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
        
        # 3. Preprocess (Returns data for both models)
        overall_data, product_data = preprocess_data(df, config)
        
        # 4. Train & Evaluate Overall Sales Model
        score_o = train_model(*overall_data, config, config['paths']['overall_model_path'])
        
        # 5. Train & Evaluate Product Sales Model
        score_p = train_model(*product_data, config, config['paths']['product_model_path'])
        
        logger.info("Pipeline Execution Completed Successfully")
        print("-" * 30)
        print(f"Pipeline Finished.")
        print(f"Overall Sales Model R2 Score: {score_o:.4f}")
        print(f"Product Sales Model R2 Score: {score_p:.4f}")
        print("-" * 30)
        
    except Exception as e:
        logger.error(f"Pipeline Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

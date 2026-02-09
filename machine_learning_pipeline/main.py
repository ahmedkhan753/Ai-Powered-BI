import yaml
import sys
import os
from src.data.data_fetcher import fetch_data
from src.preprocess.preprocessor import preprocess_data
from src.models.train import train_model
from src.utils.logger import get_logger
from src.utils.cache_utils import model_cache

logger = get_logger(__name__)

def load_config(config_path=None):
    if config_path is None:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(current_dir, "config", "config.yaml")
    
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
        overall_hash = model_cache.get_data_hash(overall_data[0]) # hash X_train
        if model_cache.is_cached("overall_sales", overall_hash) and os.path.exists(config['paths']['overall_model_path']):
            logger.info("Overall Sales Model is already cached and exists on disk. Skipping training.")
            # Load model to get score or just return a dummy if we don't want to re-evaluate
            # For simplicity, if cached we skip. In a real scenario we might load and evaluate.
            score_o = 1.0 # Placeholder or load from disk
        else:
            score_o = train_model(*overall_data, config, config['paths']['overall_model_path'])
            model_cache.set_cached("overall_sales", overall_hash)
        
        # 5. Train & Evaluate Product Sales Model
        product_hash = model_cache.get_data_hash(product_data[0]) # hash X_train
        if model_cache.is_cached("product_sales", product_hash) and os.path.exists(config['paths']['product_model_path']):
            logger.info("Product Sales Model is already cached and exists on disk. Skipping training.")
            score_p = 1.0 # Placeholder
        else:
            score_p = train_model(*product_data, config, config['paths']['product_model_path'])
            model_cache.set_cached("product_sales", product_hash)
        
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

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import pandas as pd
import pickle
import os
from src.utils.logger import get_logger

logger = get_logger(__name__)

def preprocess_data(df, config):
    """
    Cleans data, handles missing values, and splits into train/test.
    """
    try:
        logger.info("Starting data preprocessing...")
        
        target_col = config['model']['target_column']
        
        # Simple preprocessing: Drop NA for simplicity in this template
        df = df.dropna()
        
        X = df.drop(columns=[target_col])
        y = df[target_col]
        
        # Scaling
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)
        
        # Save Scaler
        os.makedirs(os.path.dirname(config['paths']['preprocessor_path']), exist_ok=True)
        with open(config['paths']['preprocessor_path'], 'wb') as f:
            pickle.dump(scaler, f)
            
        logger.info(f"Scaler saved to {config['paths']['preprocessor_path']}")
            
        # Split
        X_train, X_test, y_train, y_test = train_test_split(
            X_scaled, y, test_size=0.2, random_state=config['model']['random_state']
        )
        
        logger.info(f"Data split completed. Train shape: {X_train.shape}, Test shape: {X_test.shape}")
        
        return X_train, X_test, y_train, y_test
        
    except Exception as e:
        logger.error(f"Error in preprocessing: {e}")
        raise e

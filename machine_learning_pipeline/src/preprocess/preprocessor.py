from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import pandas as pd
import pickle
import os
from src.utils.logger import get_logger

logger = get_logger(__name__)

def preprocess_data(df, config):
    """
    Cleans data, handles categorical encoding, and prepares data for models.
    """
    try:
        logger.info("Starting data preprocessing...")
        
        # Drop rows with missing values
        df = df.dropna()
        
        # Encoding categorical variables
        # Weekday encoding (Simple mapping)
        weekday_map = {
            'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3, 
            'Friday': 4, 'Saturday': 5, 'Sunday': 6
        }
        df['weekday_num'] = df['weekday_name'].map(weekday_map)
        
        # Product Category encoding (One-hot or Label encoding - sticking to Label for simplicity)
        df['product_cat_num'] = df['product_category'].astype('category').cat.codes
        
        # Prepare Overall Sales Data (Aggregate by Year, Month, Day)
        overall_df = df.groupby(['year', 'month', 'day', 'quarter', 'weekday_num', 'is_weekend'])['sales_amount'].sum().reset_index()
        
        # Prepare Product-specific Sales Data (Aggregate by Product, Date)
        product_df = df.groupby(['product_key', 'product_cat_num', 'year', 'month', 'day', 'quarter', 'weekday_num', 'is_weekend'])['sales_amount'].sum().reset_index()
        
        # Helper to split data
        def split_and_scale(data, target_col, drop_cols):
            X = data.drop(columns=[target_col] + drop_cols)
            y = data[target_col]
            
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=config['model']['random_state']
            )
            
            # Simple Scaling
            scaler = StandardScaler()
            X_train_scaled = scaler.fit_transform(X_train)
            X_test_scaled = scaler.transform(X_test)
            
            return X_train_scaled, X_test_scaled, y_train, y_test, scaler

        # Process Overall Data
        X_train_o, X_test_o, y_train_o, y_test_o, scaler_o = split_and_scale(overall_df, 'sales_amount', [])
        
        # Process Product Data
        X_train_p, X_test_p, y_train_p, y_test_p, scaler_p = split_and_scale(product_df, 'sales_amount', [])
        
        # Save Scaler (Using overall for simplicity, or we could save both)
        os.makedirs(os.path.dirname(config['paths']['preprocessor_path']), exist_ok=True)
        with open(config['paths']['preprocessor_path'], 'wb') as f:
            pickle.dump(scaler_o, f)
            
        logger.info("Data split and scaling completed for both models.")
        
        return (X_train_o, X_test_o, y_train_o, y_test_o), (X_train_p, X_test_p, y_train_p, y_test_p)
        
    except Exception as e:
        logger.error(f"Error in preprocessing: {e}")
        raise e

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score
import pickle
import os
from src.utils.logger import get_logger

logger = get_logger(__name__)

def train_model(X_train, X_test, y_train, y_test, config, model_save_path):
    """
    Trains the model and evaluates it.
    """
    try:
        logger.info(f"Starting model training for {os.path.basename(model_save_path)}...")
        
        model = RandomForestRegressor(
            n_estimators=config['model']['n_estimators'],
            random_state=config['model']['random_state']
        )
        
        model.fit(X_train, y_train)
        
        logger.info("Model training finished.")
        
        # Evaluate
        predictions = model.predict(X_test)
        score = r2_score(y_test, predictions)
        logger.info(f"Model R2 Score for {os.path.basename(model_save_path)}: {score}")
        
        # Save Model
        os.makedirs(os.path.dirname(model_save_path), exist_ok=True)
        with open(model_save_path, 'wb') as f:
            pickle.dump(model, f)
            
        logger.info(f"Model saved to {model_save_path}")
        
        return score
        
    except Exception as e:
        logger.error(f"Error in training: {e}")
        raise e

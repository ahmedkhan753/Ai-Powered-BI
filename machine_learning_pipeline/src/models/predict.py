import pickle
import os
import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)

def make_prediction(model_path, preprocessor_path, input_data):
    """
    Loads model and preprocessor to make predictions on input data.
    """
    try:
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model not found at {model_path}")
        if not os.path.exists(preprocessor_path):
            raise FileNotFoundError(f"Preprocessor not found at {preprocessor_path}")

        # Load
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        with open(preprocessor_path, 'rb') as f:
            scaler = pickle.load(f)

        # Scale and Predict
        scaled_data = scaler.transform(input_data)
        predictions = model.predict(scaled_data)
        
        return predictions

    except Exception as e:
        logger.error(f"Error in prediction: {e}")
        raise e

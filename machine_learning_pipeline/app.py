"""
FastAPI Application for Real-time ML Inference.
This service exposes endpoints to make predictions using trained models.
"""

import os
import pickle
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from contextlib import asynccontextmanager
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Initialize FastAPI App
app = FastAPI(title="ML Inference API", version="1.0.0")

# Global variables to hold models
models = {
    "overall": None,
    "product": None,
    "preprocessor": None
}

class PredictionInput(BaseModel):
    sales_amount: float
    quantity: int
    product_key: int
    year: int
    month: int
    day: int
    quarter: int
    weekday_name: str
    is_weekend: bool

    is_weekend: bool

def load_models():
    """Load models from disk into global dictionary."""
    try:
        # Paths (hardcoded for now, ideally from config)
        base_path = "models"
        overall_path = os.path.join(base_path, "overall_sales_model.pkl")
        product_path = os.path.join(base_path, "product_sales_model.pkl")
        preprocessor_path = os.path.join(base_path, "preprocessor.pkl")

        if os.path.exists(overall_path):
            with open(overall_path, "rb") as f:
                models["overall"] = pickle.load(f)
        
        if os.path.exists(product_path):
            with open(product_path, "rb") as f:
                models["product"] = pickle.load(f)
                
        if os.path.exists(preprocessor_path):
            with open(preprocessor_path, "rb") as f:
                models["preprocessor"] = pickle.load(f)
                
        logger.info("Models loaded.")
    except Exception as e:
        logger.error(f"Error loading models: {e}")
        # Don't crash, just log. Health check will reveal issues.

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load models
    load_models()
    yield
    # Clean up
    models.clear()


app = FastAPI(title="ML Inference API", version="1.0.0", lifespan=lifespan)

@app.get("/health")
def health_check():
    status = "healthy"
    if not models["overall"] or not models["product"] or not models["preprocessor"]:
        status = "degraded"
    return {"status": status, "models_loaded": {k: v is not None for k, v in models.items()}}

@app.post("/predict")
def predict(input_data: PredictionInput):
    if not models["overall"] or not models["product"] or not models["preprocessor"]:
        raise HTTPException(status_code=503, detail="Models not loaded")
    
    try:
        # Convert input to DataFrame
        data = input_data.dict()
        df = pd.DataFrame([data])
        
        # 1. Feature Engineering (Must match preprocessor.py)
        # Weekday encoding
        weekday_map = {
            'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3, 
            'Friday': 4, 'Saturday': 5, 'Sunday': 6
        }
        df['weekday_num'] = df['weekday_name'].map(weekday_map)
        
        
        # 2. Select Columns for Model
        # The model was trained on: ['year', 'month', 'day', 'quarter', 'weekday_num', 'is_weekend']
        # The StandardScaler was fitted on these exact columns in this order (from preprocessor.py groupby)
        
        features_overall = ['year', 'month', 'day', 'quarter', 'weekday_num', 'is_weekend']
        
        # Ensure Types match
        # Groupby in preprocessor likely kept them as int/bool.
        # Pydantic schema: year(int), month(int), day(int), quarter(int), is_weekend(bool)
        # weekday_num is int (from map).
        
        # Prepare data for Overall Sales Model
        X_overall = df[features_overall].copy()
        
        # Scale
        # Note: If is_weekend is bool, sklearn scaler handles it as 0/1 usually, but safer to cast to int/float if issue persists.
        # Let's cast is_weekend to int to be safe, though scaler handles bools.
        X_overall['is_weekend'] = X_overall['is_weekend'].astype(int)
        
        X_overall_scaled = models["preprocessor"].transform(X_overall)
        
        # Predict Overall
        overall_pred = models["overall"].predict(X_overall_scaled)
        
        # Product Sales Model
        # FOR NOW: Only Return Overall Prediction to fix the immediate 500 error.
        # If we really need product prediction, we need `product_category` in input.
        
        product_pred_val = 0.0
        
        return {
            "overall_sales_prediction": float(overall_pred[0]),
            "product_sales_prediction": product_pred_val, # Placeholder until input schema is updated
            "note": "Product prediction requires product_category. Currently returning 0."
        }
    except Exception as e:


        logger.error(f"Prediction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))







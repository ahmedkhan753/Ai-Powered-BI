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
        data = input_data.dict() # pydantic v1, use model_dump for v2 but v1 is safer for now or check version
        df = pd.DataFrame([data])
        
        # Preprocess
        # Ensure column order matches training
        # Note: The preprocessor (ColumnTransformer) expects specific columns.
        # We need to make sure the input DF has the right columns in right order if required by the transformer,
        # but usually ColumnTransformer matches by name if remainder='passthrough' or specific columns are named.
        # However, checking `src/preprocess/preprocessor.py` would be ideal.
        # For now we assume standard sklearn usage.
        
        X_scaled = models["preprocessor"].transform(df)
        
        # Predict
        overall_pred = models["overall"].predict(X_scaled)
        product_pred = models["product"].predict(X_scaled)
        
        return {
            "overall_sales_prediction": float(overall_pred[0]),
            "product_sales_prediction": float(product_pred[0])
        }
    except Exception as e:
        logger.error(f"Prediction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))







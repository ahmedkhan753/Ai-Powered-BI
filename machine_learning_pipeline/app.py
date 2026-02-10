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





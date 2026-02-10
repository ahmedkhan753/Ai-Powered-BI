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

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load models
    models["overall"] = "loaded" # Placeholder
    models["product"] = "loaded" # Placeholder
    logger.info("Models loaded successfully")
    yield
    # Clean up
    models.clear()

app = FastAPI(title="ML Inference API", version="1.0.0", lifespan=lifespan)





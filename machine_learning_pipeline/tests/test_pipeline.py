import pytest
import os
from src.components.data_ingestion import DataIngestion

def test_data_ingestion_creation():
    ingestion = DataIngestion()
    assert ingestion.ingestion_config.train_data_path is not None

def test_imports():
    from src.pipelines.training_pipeline import TrainPipeline
    assert TrainPipeline is not None

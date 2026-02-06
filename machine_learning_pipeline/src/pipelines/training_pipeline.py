import sys
from src.components.data_ingestion import DataIngestion
from src.components.data_transformation import DataTransformation
from src.components.model_trainer import ModelTrainer
from src.exception import CustomException
from src.logger import logging

class TrainPipeline:
    def __init__(self):
        pass

    def start_training(self):
        try:
            logging.info("Starting Training Pipeline")
            
            # Ingestion
            obj = DataIngestion()
            train_data, test_data = obj.initiate_data_ingestion()

            # Transformation
            data_transformation = DataTransformation()
            train_arr, test_arr, _ = data_transformation.initiate_data_transformation(train_data, test_data)

            # Training
            model_trainer = ModelTrainer()
            score = model_trainer.initiate_model_trainer(train_arr, test_arr)
            
            logging.info(f"Training Pipeline Completed. Score: {score}")

        except Exception as e:
            raise CustomException(e, sys)

if __name__ == "__main__":
    pipeline = TrainPipeline()
    pipeline.start_training()

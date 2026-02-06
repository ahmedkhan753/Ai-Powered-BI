import os
import sys
from dataclasses import dataclass

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score
import pickle

from src.exception import CustomException
from src.logger import logging

@dataclass
class ModelTrainerConfig:
    trained_model_file_path = os.path.join("artifacts", "model.pkl")

class ModelTrainer:
    def __init__(self):
        self.model_trainer_config = ModelTrainerConfig()

    def initiate_model_trainer(self, train_array, test_array):
        try:
            logging.info("Split training and test input data")
            X_train, y_train, X_test, y_test = (
                train_array[:, :-1],
                train_array[:, -1],
                test_array[:, :-1],
                test_array[:, -1],
            )
            
            model = RandomForestRegressor()
            model.fit(X_train, y_train)

            logging.info("Model training finished")

            predicted = model.predict(X_test)
            r2_square = r2_score(y_test, predicted)
            
            logging.info(f"Best model score (R2): {r2_square}")
            
            # Save model
            os.makedirs(os.path.dirname(self.model_trainer_config.trained_model_file_path), exist_ok=True)
            with open(self.model_trainer_config.trained_model_file_path, "wb") as file_obj:
                pickle.dump(model, file_obj)

            return r2_square
            
        except Exception as e:
            raise CustomException(e, sys)

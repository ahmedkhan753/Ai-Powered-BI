from src.pipelines.training_pipeline import TrainPipeline
from src.logger import logging

if __name__ == "__main__":
    try:
        logging.info("Main entry point triggered")
        pipeline = TrainPipeline()
        pipeline.start_training()
    except Exception as e:
        logging.error(f"Main execution failed: {e}")
        raise e

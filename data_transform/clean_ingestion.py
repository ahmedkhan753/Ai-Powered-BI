from data_transform.load_clean_data import load_clean_data, extract_clean_data
import logging


def clean_ingestion_pipeline():
    logging.info("Starting Silver Layer incremental pipeline...")

    # Use the incremental extraction logic already implemented in load_clean_data.py
    cleaned_data = extract_clean_data()

    if cleaned_data is not None and not cleaned_data.empty:
        load_clean_data(cleaned_data)
        logging.info(
            f"Successfully processed and loaded {len(cleaned_data)} new records."
        )
    else:
        logging.info("No new data to process for Silver Layer.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    clean_ingestion_pipeline()

from data_transform.raw_data_fetcher import raw_data_fetcher
from data_transform.raw_data_fetcher import engine, query
import pandas as pd
from data_transform.load_clean_data import load_clean_data, extract_clean_data
from data_transform.clean_sales import CleanSales

def clean_ingestion_pipeline():
    raw_data = raw_data_fetcher(query, engine)
    cleaner = CleanSales(raw_data)
    cleaned_data = cleaner.clean_data()
    load_clean_data(cleaned_data)

if __name__ == "__main__":
    clean_ingestion_pipeline()
    print("Data cleaning and loading pipeline executed successfully!")
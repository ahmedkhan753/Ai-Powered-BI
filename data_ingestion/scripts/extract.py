
import pandas as pd
import os

def extract_data(file_path: str) -> pd.DataFrame:
    """
    Extracts the full raw data from the CSV file.
    Raises an error if the file doesn't exist.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    df = pd.read_csv(file_path)
    
    print(f"Extracted {len(df)} rows from {os.path.basename(file_path)}")
    return df

if __name__ == "__main__":
    FILE_PATH = "source/Sales_Dataset.csv" 
    try:
        df = extract_data(FILE_PATH)
        print(df.head())
        print("\nData types:")
        print(df.dtypes)
    except Exception as e:
        print(f"Error: {e}")
from extract import extract_data

def validate_data(data):
    if data.empty:
        raise ValueError("The provided data is empty.")
    required_columns = ['OrderID', 'Product', 'Quantity', 'Price']
    for col in required_columns:
        if col not in data.columns:
            raise ValueError(f"Required column '{col}' is missing from the data.")
        

if __name__ == "__main__":
    file_path = "source/Sales_Dataset.csv"
    try:
        df = extract_data(file_path)
        validate_data(df)
        print("Data validation successful.")
    except Exception as e:
        print(e)
        

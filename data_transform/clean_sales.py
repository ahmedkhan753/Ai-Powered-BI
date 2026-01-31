from raw_data_fetcher import raw_data_fetcher
from raw_data_fetcher import engine, query
import pandas as pd
import numpy as np


class CleanSales:
    def __init__(self, raw_data_fetcher: pd.DataFrame):
        self.raw_data = raw_data_fetcher

    def clean_data(self) -> pd.DataFrame:
        df = self.raw_data.copy()

        # Rename columns to consistent naming
        df.rename(columns={
            'date': 'sale_date',
            'age': 'customer_age',
            'total_amount': 'sales_amount'
        }, inplace=True)

        # Drop duplicates
        df.drop_duplicates(inplace=True)

        # Handle missing values
        df.fillna({
            'customer_age': df['customer_age'].median() if 'customer_age' in df.columns else 0,
            'gender': 'Unknown',
            'product': 'Unknown',
            'quantity': 0,
            'price': 0.0,
            'sales_amount': 0.0
        }, inplace=True)

        # Convert data types
        df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce')
        df['customer_age'] = pd.to_numeric(df['customer_age'], errors='coerce').astype(int)
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').astype(int)
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['sales_amount'] = pd.to_numeric(df['sales_amount'], errors='coerce')

        # Filter out invalid values
        df = df[df['customer_age'].between(0, 120)]
        df = df[df['sales_amount'] >= 0]
        df = df[df['quantity'] >= 0]
        df = df[df['price'] >= 0]

        # Created a new column for money spent categorization
        

        median_spent = df['sales_amount'].median()

        df['spend_category'] = np.where(
        df['sales_amount'] > median_spent,'A','B'
        )

        return df
    

if __name__ == "__main__":
    raw_data = raw_data_fetcher(query, engine)
    cleaner = CleanSales(raw_data)
    cleaned_data = cleaner.clean_data()
    print(cleaned_data.head())

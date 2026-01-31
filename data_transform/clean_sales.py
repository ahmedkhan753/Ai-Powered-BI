from raw_data_fetcher import raw_data_fetcher
import pandas as pd



class clean_sales:
    def __init__(self, raw_data_fetcher: pd.DataFrame):
        self.raw_data = raw_data_fetcher

    def clean_data(self) -> pd.DataFrame:
        df = self.raw_data.copy()

        # Remove first unnecessary column
        if df.columns[0] == 'unnamed_column':
            df.drop(columns=df.columns[0], inplace=True)

        # Drop duplicates
        df.drop_duplicates(inplace=True)

        # Handle missing values
        df.fillna({
            'sales_amount': 0,
            'customer_id': 'unknown',
            'product_id': 'unknown'
        }, inplace=True)

        # Convert data types
        df['sales_amount'] = pd.to_numeric(df['sales_amount'], errors='coerce').fillna(0)
        df['sale_date'] = pd.to_datetime(df['sale_date'], errors='coerce')

        # Filter out invalid sales amounts
        df = df[df['sales_amount'] >= 0]

        # Age of customer
        if 'customer_age' in df.columns:
            df['customer_age'] = pd.to_numeric(df['customer_age'], errors='coerce')
            df = df[(df['customer_age'] >= 0) & (df['customer_age'] <= 120)]

        return df
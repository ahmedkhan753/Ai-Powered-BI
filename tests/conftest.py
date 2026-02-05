import pytest
import pandas as pd
import os
import sys

# Add the scripts directory to the python path so imports work
sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../data_ingestion/scripts")
    )
)


@pytest.fixture
def sample_sales_df():
    """Returns a valid sales dataframe for testing."""
    return pd.DataFrame(
        {
            "OrderID": [1001, 1002, 1003],
            "Date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "Gender": ["Male", "Female", "Male"],
            "Age": [30, 25, 35],
            "Product": ["Widget A", "Widget B", "Widget C"],
            "Quantity": [2, 1, 3],
            "Price": [10.0, 15.0, 5.0],
            "Total Amount": [20.0, 15.0, 15.0],
        }
    )


@pytest.fixture
def sample_csv_path():
    """Returns the path to the sample CSV file."""
    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), "data/sample_sales.csv")
    )

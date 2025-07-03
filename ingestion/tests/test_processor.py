import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import pytest
import numpy as np

from app.processor import DataProcessor

@pytest.fixture
def processor_intance():
    return DataProcessor()

@pytest.fixture
def sample_dataframe():
    return pd.DataFrame({
        "Amount": [100.0, 200.0, 300.0, 100.0, np.nan],
        "Time": [10.0, 20.0, 30.0, 10.0, 50.0],
        "Class": [0, 1, 0, 0, 1],
        "V1": [1.0, 2.0, 3.0, 1.0, 5.0],
        "V2": [1.1, 2.2, 3.3, 1.1, 5.5]
    })

@pytest.fixture
def dataframe_with_duplicates():
    return pd.DataFrame({
        "Amount": [100.0, 200.0, 100.0],
        "Time": [10.0, 20.0, 10.0],
        "Class": [0, 1, 0]
    })

@pytest.fixture
def dataframe_with_nans():
    return pd.DataFrame({
        "Amount": [100.0, np.nan, 300.0],
        "Time": [10.0, 20.0, np.nan],
        "Class": [0, 1, 0]
    })

@pytest.fixture
def dataframe_with_mixed_types():
    return pd.DataFrame({
        "Amount": ["100", "200", "300"], # Strings that should be float
        "Time": [10, 20, 30],
        "Class": [0.0, 1.0, 0.0] # Floats that should be int
    })

def test_clean_data():
    processor = DataProcessor()
    df = pd.DataFrame({
        "Amount": [100, 200, 300],
        "Time": [10, 20, 30],
        "Class": [0, 1, 0]
    })
    processed_df = processor.clean_data(df.copy())
    assert "Amount" in processed_df.columns, "Amount column should be present after cleaning"
    assert "Time" in processed_df.columns
    assert not processed_df.isnull().any().any(), "Data should not contain NaN values after cleaning"
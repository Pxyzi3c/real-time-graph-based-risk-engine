import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from app.processor import DataProcessor

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
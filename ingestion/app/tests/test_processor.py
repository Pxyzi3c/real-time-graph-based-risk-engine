import pandas as pd
from ingestion.app.processor import DataProcessor

def test_clean_kaggle():
    processor = DataProcessor()
    df = pd.DataFrame({
        "Amount": [100, 200, 300],
        "Time": [10, 20, 30],
        "Class": [0, 1, 0]
    })
    processed_df = processor.clean_data(df.copy())
    assert "Amount" in processed_df.columns
    assert "Time" in processed_df.columns
    assert not processed_df.isnull().any().any(), "Data should not contain NaN values after cleaning"
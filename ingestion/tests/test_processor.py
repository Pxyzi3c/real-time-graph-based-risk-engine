import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import pytest
import numpy as np

from app.processor import DataProcessor

@pytest.fixture
def processor_instance():
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
        "Amount": ["100", "200", "300"],
        "Time": [10, 20, 30],
        "Class": [0.0, 1.0, 0.0]
    })

def test_clean_data_removes_nans(processor_instance, dataframe_with_nans):
    initial_rows = len(dataframe_with_nans)
    processed_df = processor_instance.clean_data(dataframe_with_nans.copy())

    assert not processed_df.isnull().any().any(), "DataFrame should not contain any NaN values after cleaning."
    assert len(processed_df) < initial_rows, "Rows with NaNs should have been dropped."
    assert len(processed_df) == 1

def test_clean_data_removes_duplicates(processor_instance, dataframe_with_duplicates):
    initial_rows = len(dataframe_with_duplicates)
    processed_df = processor_instance.clean_data(dataframe_with_duplicates.copy())

    assert len(processed_df) < initial_rows, "Duplicate rows should have been dropped."
    assert len(processed_df) == 2

def test_clean_data_preserves_columns(processor_instance, sample_dataframe):
    expected_columns = ["Amount", "Time", "Class", "V1", "V2"]
    processed_df = processor_instance.clean_data(sample_dataframe.copy())

    for col in expected_columns:
        assert col in processed_df.columns, f"Column '{col}' should be present after cleaning."

def test_clean_data_with_no_nans_or_duplicates(processor_instance):
    df_clean = pd.DataFrame({
        "Amount": [100.0, 200.0],
        "Time": [10.0, 20.0],
        "Class": [0, 1]
    })
    initial_rows = len(df_clean)
    processed_df = processor_instance.clean_data(df_clean.copy())

    assert len(processed_df) == initial_rows, "No rows should be dropped if no NaNs or duplicates."
    assert not processed_df.isnull().any().any(), "DataFrame should remain NaN-free."
    pd.testing.assert_frame_equal(processed_df, df_clean)

# --- Test Cases for feature_engineering method ---

def test_feature_engineering_class_astype_int(processor_instance, dataframe_with_mixed_types):
    clean_df = processor_instance.clean_data(dataframe_with_mixed_types.copy())
    
    processed_df = processor_instance.feature_engineering(clean_df.copy())
    
    assert processed_df['Class'].dtype == 'int32' or processed_df['Class'].dtype == 'int64', \
        f"Class column should be integer type, but is {processed_df['Class'].dtype}"

def test_feature_engineering_preserves_other_columns(processor_instance, sample_dataframe):
    clean_df = processor_instance.clean_data(sample_dataframe.copy())
    original_v1 = clean_df['V1'].copy()
    original_v2 = clean_df['V2'].copy()

    processed_df = processor_instance.feature_engineering(clean_df.copy())

    pd.testing.assert_series_equal(processed_df['V1'], original_v1, check_dtype=True)
    pd.testing.assert_series_equal(processed_df['V2'], original_v2, check_dtype=True)
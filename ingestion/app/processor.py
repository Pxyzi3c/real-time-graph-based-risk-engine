import pandas as pd
import logging
import json
import hashlib

from sklearn.preprocessing import StandardScaler

from config.logging_config import setup_logging
setup_logging()

logger = logging.getLogger(__name__)

class DataProcessor:
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Starting data cleaning...")
        # Example cleaning steps
        original_rows = len(df)
        df = df.dropna()
        logger.info(f"Dropped {original_rows - len(df)} rows with missing values.")
        df = df.drop_duplicates()
        logger.info(f"Dropped {original_rows - len(df)} duplicates rows.")
        logger.info("Data cleaning completed.")
        return df
    
    def generate_transaction_id(self, row):
        features_to_hash ={
            'Time': row['Time'],
            'Amount': row['Amount'],
            **{f"V{i}": row[f"V{i}"] for i in range(1, 14)}
        }
        unique_string = json.dumps(features_to_hash, sort_keys=True)
        return hashlib.sha256(unique_string.encode('utf-8')).hexdigest()
    
    def feature_engineering(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Starting feature engineering...")
        scaler = StandardScaler()
        df['transaction_id'] = df.apply(self.generate_transaction_id, axis=1)
        df['Amount'] = scaler.fit_transform(df[['Amount']])
        df['Time'] = scaler.fit_transform(df[['Time']])
        df['Class'] = df['Class'].astype('int')
        logger.info("Feature engineering completed.")
        return df
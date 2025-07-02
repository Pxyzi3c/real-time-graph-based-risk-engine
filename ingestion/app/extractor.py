import os
import pandas as pd
import logging
import sys

script_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(script_dir, '..', '..'))
sys.path.append(project_root)

from db import save_dataframe_to_db
from processor import DataProcessor
from pandera import DataFrameSchema, Column
import pandera.pandas as pa

from config.logging_config import setup_logging
from config.settings import settings

setup_logging()
logger = logging.getLogger(__name__)

class DataExtractor:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = input_path
        self.output_path = output_path

    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        expected_cols = {'Time', 'Amount', 'Class', *[f'V{i}' for i in range(1, 28)]}
        if not expected_cols.issubset(set(df.columns)):
            logger.error(f"Missing expected columns: {expected_cols - set(df.columns)}")
            raise ValueError("Data schema does not match expected Kaggle structure.")
        return df
        # return DataFrameSchema({
        #         "Time": Column(pa.Float),
        #         "Amount": Column(pa.Float),
        #         "Class": Column(pa.Int),
        #         **{f"V{i}": Column(pa.Float) for i in range(1, 29)}
        #     }).validate(df)

    def extract(self) -> pd.DataFrame:
        if not os.path.exists(self.input_path):
            logger.critical(f"Data file not found at {self.input_path}")
            raise FileNotFoundError(f"Data file not found at: {self.input_path}")

        file_size_bytes = os.path.getsize(self.input_path)
        logger.info(f"Input file '{self.input_path}' size: {file_size_bytes / (1024 * 1024):.2f} MB")
        
        try:
            df = pd.read_csv(self.input_path)
            logger.info(f"Data extracted successfully from {self.input_path}")
            self.validate_data(df)
            return df
        except FileNotFoundError as e:
            logger.error(f"File not found: {self.input_path}")
            raise e
        except Exception as e:
            logger.error(f"Error while reading the CSV file: {e}")
            raise e

class DataIngestion:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = input_path
        self.output_path = output_path
        self.extractor = DataExtractor(self.input_path, self.output_path)
        self.processor = DataProcessor()
        self.logger = logger

    def run(self):
        try:
            # Step 1: Extract data
            df = self.extractor.extract()

            # Step 2: Clean the data
            df = self.processor.clean_data(df)  

            # Step 3: Feature engineering
            df = self.processor.feature_engineering(df)

            # Step 4: Save to PostgreSQL
            save_dataframe_to_db(df, 'credit_card_fraud', if_exists='replace', chunksize=settings.DB_SAVE_CHUNKSIZE if hasattr(settings, 'DB_SAVE_CHUNKSIZE') else 10000)

            # Step 4: Save the processed data
            df.reset_index(drop=True, inplace=True)
            df.to_csv(self.output_path, index=False)
            logger.info(f"Data processing complete. Data saved to {self.output_path}")
        except Exception as e:
            logger.error(f"Data ingestion failed: {e}")
            raise e
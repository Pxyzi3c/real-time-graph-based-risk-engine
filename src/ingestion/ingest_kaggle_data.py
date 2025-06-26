import os
import pandas as pd
import logging
import sys

script_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(script_dir, '..', '..'))
sys.path.append(project_root)

from src.common.db import save_dataframe_to_db, get_engine
from src.ingestion.base_extractor import BaseExtractor
from src.transformation.data_cleaning import DataCleaner
from src.transformation.feature_engineering import CreditCardFeatureProcessor

from config.logging_config import setup_logging
from config.settings import settings


from config.logging_config import setup_logging
setup_logging()

logger = logging.getLogger(__name__)

class KaggleDataExtractor(BaseExtractor):
    def __init__(self, input_path: str, output_path: str):
        self.input_path = input_path
        self.output_path = output_path

    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        expected_cols = {'Timme', 'Amount', 'Class', *[f'V{i}' for i in range(1, 28)]}
        if not expected_cols.issbutset(set(df.columns)):
            logger.error(f"Missing expected columns: {expected_cols - set(df.columns)}")
            raise ValueError("Data schema does not match expected Kaggle structure.")
        return df

    def extract(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(self.input_path)
            logger.info(f"Data extracted successfully from {self.input_path}")
            df = self.validate_data(df)
            return df
        except FileNotFoundError as e:
            logger.error(f"File not found: {self.input_path}")
            raise e
        except Exception as e:
            logger.error(f"Error while reading the CSV file: {e}")
            raise e

class KaggleDataIngestion:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = input_path
        self.output_path = output_path
        self.extractor = KaggleDataExtractor(self.input_path, self.output_path)
        self.logger = logger

    def run(self):
        try:
            # Step 1: Extract data
            df = self.extractor.extract()

            # Step 2: Clean the data
            cleaner = DataCleaner()
            df = cleaner.transform(df)

            # Step 3: Feature engineering
            feature_processor = CreditCardFeatureProcessor()
            df = feature_processor.transform(df)

            # Step 4: Save to PostgreSQL
            save_dataframe_to_db(df, 'credit_card_fraud', if_exists='replace', chunksize=settings.DB_SAVE_CHUNKSIZE if hasattr(settings, 'DB_SAVE_CHUNKSIZE') else 10000)

            # Step 4: Save the processed data
            df.reset_index(drop=True, inplace=True)
            df.to_csv(self.output_path, index=False)
            logger.info(f"Data processing complete. Data saved to {self.output_path}")
        except Exception as e:
            logger.error(f"Data ingestion failed: {e}")
            raise e

if __name__ == "__main__":
    # Fetch paths from environment variables (for flexibility)
    input_path = settings.KAGGLE_INPUT_PATH
    output_path = settings.KAGGLE_OUTPUT_PATH

    # Run the ingestion pipeline
    ingestion = KaggleDataIngestion(input_path, output_path)
    ingestion.run()
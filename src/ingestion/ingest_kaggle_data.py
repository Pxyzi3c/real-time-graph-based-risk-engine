import sys
import os

script_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(script_dir, '..', '..'))
sys.path.append(project_root)

import logging
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from src.ingestion.base_extractor import BaseExtractor
from src.transformation.data_cleaning import DataCleaner
from src.transformation.feature_engineering import CreditCardFeatureProcessor
from config.logging_config import setup_logging

load_dotenv()

from config.logging_config import setup_logging
setup_logging()

logger = logging.getLogger(__name__)

class KaggleDataExtractor(BaseExtractor):
    def __init__(self, input_path: str, output_path: str):
        self.input_path = input_path
        self.output_path = output_path

    def extract(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(self.input_path)
            logger.info(f"Data extracted successfully from {self.input_path}")
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

    def save_to_postgres(self, df: pd.DataFrame):
        try:
            engine = create_engine(os.getenv("DATABASE_URL"))

            df.to_sql('credit_card_fraud', engine, index=False, if_exists='replace', method='multi')
            logger.info("Data saved to PostgreSQL successfully.")
        except Exception as e:
            logger.error(f"Error while saving data to PostgreSQL: {e}")
            raise e

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
            self.save_to_postgres(df)

            # Step 4: Save the processed data
            df.reset_index(drop=True, inplace=True)
            df.to_csv(self.output_path, index=False)
            logger.info(f"Data processing complete. Data saved to {self.output_path}")
        except Exception as e:
            logger.error(f"Data ingestion failed: {e}")
            raise e

if __name__ == "__main__":
    # Fetch paths from environment variables (for flexibility)
    input_path = os.getenv("KAGGLE_INPUT_PATH", "data/raw/creditcard.csv")
    output_path = os.getenv("KAGGLE_OUTPUT_PATH", "data/processed/creditcard_processed.csv")

    # Run the ingestion pipeline
    ingestion = KaggleDataIngestion(input_path, output_path)
    ingestion.run()
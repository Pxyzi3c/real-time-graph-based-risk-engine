import os
import pandas as pd
import logging
import pandera.pandas as pa

from pandera import DataFrameSchema, Column
from config.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

class DataExtractor:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = input_path
        self.output_path = output_path

    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        return DataFrameSchema({
                "Time": Column(pa.Float),
                "Amount": Column(pa.Float),
                "Class": Column(pa.Int),
                **{f"V{i}": Column(pa.Float) for i in range(1, 29)}
            }).validate(df)

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
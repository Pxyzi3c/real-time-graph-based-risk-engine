from src.transformation.base_transformer import BaseTransformer
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class DataCleaner(BaseTransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Starting data cleaning...")
        # Example cleaning steps
        original_rows = len(df)
        df.dropna(inplace=True)
        logger.info(f"Dropped {original_rows - len(df)} rows with missing values.")
        df.drop_duplicates(inplace=True)
        logger.info(f"Dropped {original_rows - len(df)} duplicates rows.")
        # ... more cleaning logic
        return df
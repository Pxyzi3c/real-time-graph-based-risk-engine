import os
import pandas as pd
import logging

class KaggleDataExtractor:
    def __init__(self, filepath: str):
        self.filepath = filepath

    def extract(self) -> pd.DataFrame:
        try:
            df = pd.read_csv(self.filepath)
            logging.info(f"✅ Extracted dataset from {self.filepath} with shape: {df.shape}")
            return df
        except Exception as e:
            logging.error(f"❌ Extraction failed: {e}")
            raise
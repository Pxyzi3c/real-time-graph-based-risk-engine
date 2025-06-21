import pandas as pd
from sklearn.preprocessing import StandardScaler
from src.transformation.base_transformer import BaseTransformer
import logging

from config.logging_config import setup_logging
setup_logging()
logger = logging.getLogger(__name__)

class CreditCardFeatureProcessor(BaseTransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Starting feature processing...")
        scaler = StandardScaler()
        df['Amount'] = scaler.fit_transform(df[['Amount']])
        df['Time'] = scaler.fit_transform(df[['Time']])
        df['Class'] = df['Class'].astype('int')
        return df
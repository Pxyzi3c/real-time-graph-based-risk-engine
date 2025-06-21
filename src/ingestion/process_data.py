import sys
import os

script_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(script_dir, '..', '..'))
sys.path.append(project_root)

import pandas as pd
from src.transformation.data_cleaning import DataCleaner
from src.transformation.feature_engineering import CreditCardFeatureProcessor

def main():
    df = pd.read_csv('data/raw/creditcard.csv')

    # Data Cleaning
    cleaner = DataCleaner()
    df = cleaner.transform(df)

    # Feature Scaling
    feature_processor = CreditCardFeatureProcessor()
    df = feature_processor.transform(df)

    df.reset_index(drop=True, inplace=True)

    # Save Data
    df.to_csv('data/processed/creditcard.csv', index=False)

main()
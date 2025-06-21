import sys
import os

script_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(script_dir, '..', '..'))
sys.path.append(project_root)

import pandas as pd
from sklearn.preprocessing import StandardScaler
from src.transformation.data_cleaning import DataCleaner

def main():
    df = pd.read_csv('data/raw/creditcard.csv')

    # Data Cleaning
    cleaner = DataCleaner()
    df = cleaner.transform(df)
    print(df.shape)
    return
    # Feature Scaling
    scaler = StandardScaler()
    df['Amount'] = scaler.fit_transform(df[['Amount']])
    df['Class'] = df['Class'].astype('int')

    # Save Data
    df.to_csv('data/processed/creditcard.csv', index=False)

main()
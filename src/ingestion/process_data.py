"""
This script loads credit card transaction data,
performs basic data cleaning (checking for nulls and dropping rows with missing values)
"""
import pandas as pd
from sklearn.preprocessing import StandardScaler

def main():
    df = pd.read_csv('data/raw/creditcard.csv')

    # Data Cleaning
    print(df.isnull().sum())
    df.dropna(inplace=True)
    
    # Feature Scaling
    scaler = StandardScaler()
    df['Amount'] = scaler.fit_transform(df[['Amount']])
    df['Class'] = df['Class'].astype('int')

main()
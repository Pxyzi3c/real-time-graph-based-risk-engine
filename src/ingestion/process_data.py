import pandas as pd
from sklearn.preprocessing import StandardScaler

def read_data():

def main():
    df = pd.read_csv('data/raw/creditcard.csv')

    # Data Cleaning
    print(df.isnull().sum())
    df.dropna(inplace=True)
    
    # Feature Scaling
    scaler = StandardScaler()
    df['Amount'] = scaler.fit_transform(df[['Amount']])
    df['Class'] = df['Class'].astype('int')

    # Save Data
    df.to_csv('data/processed/creditcard.csv', index=False)

main()
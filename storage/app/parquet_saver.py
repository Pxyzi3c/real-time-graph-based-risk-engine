import pandas as pd
from datetime import datetime
import os

def save_to_parquet(df: pd.DataFrame, base_dir='data/parquet_logs'):
    os.makedirs(base_dir, exist_ok=True)
    file_path = os.path.join(base_dir, f"transactions_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.parquet")
    df.to_parquet(file_path, index=False)
    return file_path
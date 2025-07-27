import pandas as pd
import os

def save_to_parquet(df: pd.DataFrame, filename: str):
    output_dir = "./storage/parquet_logs"
    os.makedirs(output_dir, exist_ok=True)
    full_path = os.path.join(output_dir, filename)
    df.to_parquet(full_path, index=False)
    print(f"✅ Saved to {full_path}")
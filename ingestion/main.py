from src.ingestion.ingest_kaggle_data import KaggleDataIngestion
import os
from dotenv import load_dotenv

load_dotenv()

if __name__=="__main__":
    ingestion = KaggleDataIngestion(
        input_path=os.getenv("KAGGLE_INPUT_PATH"),
        output_path=os.getenv("KAGGLE_OUTPUT_PATH")
    )
    ingestion.run()
import logging
import argparse
import pandas as pd
import random

from app.db import save_dataframe_to_db
from app.processor import DataProcessor
from app.extractor import DataExtractor
from config.logging_config import setup_logging
from config.settings import settings
from faker import Faker
from datetime import datetime

setup_logging()
logger = logging.getLogger(__name__)
fake = Faker()

class Ingestion:
    def __init__(self, input_path: str, output_path: str):
        self.input_path = input_path
        self.output_path = output_path
        self.extractor = DataExtractor(self.input_path, self.output_path)
        self.processor = DataProcessor()
        self.logger = logger

    def kaggle(self) -> pd.DataFrame:
        try:
            # Step 1: Extract data
            self.logger.info(f"Starting data extraction from {self.input_path}")
            df = self.extractor.extract()

            # Step 2: Clean the data
            df = self.processor.clean_data(df)  

            # Step 3: Feature engineering
            df = self.processor.feature_engineering(df)

            # Step 4: Save to PostgreSQL
            save_dataframe_to_db(df, 'credit_card_fraud', if_exists='replace', chunksize=settings.DB_SAVE_CHUNKSIZE if hasattr(settings, 'DB_SAVE_CHUNKSIZE') else 10000)

            # Step 4: Save the processed data
            df.reset_index(drop=True, inplace=True)
            df.to_csv(self.output_path, index=False)
            logger.info(f"Data processing complete. Data saved to {self.output_path}")
            return df
        except Exception as e:
            logger.error(f"Data ingestion failed: {e}")
            raise e
    
    def synthetic_companies(self, num_records: int) -> pd.DataFrame:
        logger.info(f"Generating {num_records} synthetic ownership links...")
        data = []

        jurisdictions = ['us_ca', 'gb', 'au', 'de', 'fr']
        entity_types = ['company', 'individual', 'trust']
        roles = ['shareholder', 'beneficial_owner', 'director']

        for _ in range(num_records):
            jurisdiction = random.choice(jurisdictions)
            company_number = str(fake.random_int(min=10000000, max=99999999))
            data.append({
                "company_number": company_number,
                "jurisdiction_code": jurisdiction,
                "company_name": fake.company(),
                "source_url": fake.url(),
                "source_description": fake.bs(),
                "relationship_type": random.choice(["ownership", "control"]),
                "related_entity_name": fake.name() if random.random() < 0.5 else fake.company(),
                "related_entity_type": random.choice(entity_types),
                "related_entity_role": random.choice(roles),
                "retrieved_at": datetime.now()
            })

        logger.info(f"Generated {num_records} synthetic ownership links:\n {data}")
        
        df = pd.DataFrame(data)

        try:
            save_dataframe_to_db(df, "company_ownership_links", if_exists="append", chunksize=settings.DB_SAVE_CHUNKSIZE if hasattr(settings, 'DB_SAVE_CHUNKSIZE') else 10000)
            logger.info("Synthetic ownership records successfully written to PostgreSQL.")
            return df
        except Exception as e:
            logger.error(f"Data ingestion failed: {e}")
            raise e
    
if __name__ == "__main__":
    ingest = Ingestion(settings.KAGGLE_INPUT_PATH, settings.KAGGLE_OUTPUT_PATH)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source", 
        choices=["kaggle", "synthetic_companies"],
        required=True,
        help="Specify the data source for ingestion."
    )
    parser.add_argument(
        "--num_records",
        type=int,
        default=1000,
        help="Number of synthetic records to generate (only applicable for --source synthetic). Default is 1000."
    )

    args = parser.parse_args()

    if args.source == "kaggle":
        ingest.kaggle()
    elif args.source == "synthetic":
        ingest.synthetic_companies(args.num_records)
    else:
        raise ValueError(f"Invalid source specified: {args.source}")
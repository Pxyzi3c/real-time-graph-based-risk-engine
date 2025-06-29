import os
import pandas as pd
import logging
import sys
import random

script_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(script_dir, '..', '..'))
sys.path.append(project_root)

from faker import Faker
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from src.common.db import get_engine
from config.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)
fake = Faker()

class SyntheticOwnershipGenerator:
    def __init__(self, num_records: int = 100):
        self.num_records = num_records
        self.engine = get_engine()

    def generate(self) -> pd.DataFrame:
        logger.info(f"Generating {self.num_records} synthetic ownership links...")
        data = []

        jurisdictions = ['us_ca', 'gb', 'au', 'de', 'fr']
        entity_types = ['company', 'individual', 'trust']
        roles = ['shareholder', 'beneficial_owner', 'director']

        for _ in range(self.num_records):
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
        
        df = pd.DataFrame(data)
        return df
    
    def store(self, df: pd.DataFrame):
        try:
            df.to_sql("company_ownership_links", self.engine, index=False, if_exists="append", method="multi")
            logger.info(f"{len(df)} synthetic ownership records successfully written to PostgreSQL.")
        except SQLAlchemyError as e:
            logger.error(f"Failed to write synthetic records: {e}")
            raise e
        
if __name__ == "__main__":
    generator = SyntheticOwnershipGenerator(num_records=200)
    df = generator.generate()
    generator.store(df)
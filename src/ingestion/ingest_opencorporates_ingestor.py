import os
import logging 
import requests
import pandas as pd
from time import sleep
from sqlalchemy import text
from dotenv import load_dotenv
from src.common.db import get_engine
from src.ingestion.base_extractor import BaseExtractor
from config.logging_config import setup_logging
from config.settings import settings


load_dotenv()
setup_logging()
logger = logging.getLogger(__name__)

class OpenCorporatesIngestor(BaseExtractor):
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
        self.engine = get_engine()

    def get_company_links(self, jurisdiction: str, company_number: str) -> list[dict]:
        url = f"{self.base_url}/companies/{jurisdiction}/{company_number}/relationships?api_token={self.api_key}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            return data.get("results", {}).get("relationships", [])
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            return []
        
    def extract_and_store(self, jurisdiction: str, company_number: str):
        records = self.get_company_links(jurisdiction, company_number)

        if not records:
            logger.warning(f"No ownership records for {jurisdiction}/{company_number}")
            return
        
        df = pd.DataFrame([{
            "company_number": company_number,
            "jurisdiction_code": jurisdiction,
            "company_name": rel.get("company", {}).get("name"),
            "source_url": rel.get("source", {}).get("url"),
            "source_description": rel.get("source", {}).get("description"),
            "relationship_type": rel.get("relationship", {}).get("type"),
            "related_entity_name": rel.get("related_entity", {}).get("name"),
            "related_entity_type": rel.get("related_entity", {}).get("type"),
            "related_entity_role": rel.get("relationship", {}).get("role"),
        } for rel in records])

        try:
            df.to_sql("company_ownership_links", self.engine, index=False, if_exists="append", method="multi", chunksize=settings.DB_SAVE_CHUNKSIZE if hasattr(settings, 'DB_SAVE_CHUNKSIZE') else 10000)
            logger.info(f"{len(df)} ownership records saved for {jurisdiction}/{company_number}")
        except Exception as e:
            logger.error(f"Failed to save ownership records to DB: {e}")
            raise

if __name__ == "__main__":
    api_key = settings.OPENCORPORATES_API_KEY
    base_url = settings.OPENCORPORATES_BASE_URL

    ingestor = OpenCorporatesIngestor(api_key, base_url)

    test_companies = [
        {
            "jurisdiction": "gb",
            "company_number": "07495895"
        },
        {
            "jurisdiction": "us_ca",
            "company_number":  "C4305344"
        }
    ]

    for company in test_companies:
        ingestor.extract_and_store(company["jurisdiction"], company["company_number"])
        sleep(1)
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
    def __init__(self):
        self.api_key = settings.OPENCORPORATES_API_KEY
        self.base_url = settings.OPENCORPORATES_BASE_URL
        self.engine = get_engine()

    def get_company_data(self, jurisdiction: str, company_number: str) -> list[dict]:
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
            "company"
        }])
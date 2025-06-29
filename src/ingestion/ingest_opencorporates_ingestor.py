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

    def get_company_data(self, jurisdiction: str, co)
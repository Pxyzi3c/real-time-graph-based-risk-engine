import os

from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    TRANSACTIONS_TOPIC: str = os.getenv("TRANSACTIONS_TOPIC")

    KAGGLE_INPUT_PATH: str = os.getenv("KAGGLE_INPUT_PATH", "../data/raw/creditcard.csv")
    KAGGLE_OUTPUT_PATH: str = os.getenv("KAGGLE_OUTPUT_PATH", "../data/processed/creditcard_processed.csv")
    
    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'
        extra = 'ignore'

settings = Settings()
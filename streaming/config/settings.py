import os

from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    APP_NAME: str = "Real-Time Graph-Based Risk Engine Streaming"
    LOG_LEVEL: str = "INFO"

    KAFKA_BROKER_ADDRESS: str = os.getenv("KAFKA_BROKER_ADDRESS")
    KAFKA_TRANSACTIONS_TOPIC: str = os.getenv("KAFKA_TRANSACTIONS_TOPIC")
    KAFKA_OWNERSHIP_GRAPH_TOPIC: str = os.getenv("KAFKA_OWNERSHIP_GRAPH_TOPIC")
    KAFKA_ENRICHED_TRANSACTIONS_TOPIC: str = os.getenv("KAFKA_ENRICHED_TRANSACTIONS_TOPIC")
    KAFKA_CONSUMER_GROUP_ID: str = os.getenv("KAFKA_CONSUMER_GROUP_ID")
    
    POSTGRES_DB: str = os.getenv("POSTGRES_DB")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER")
    POSTGRES_PASS: str = os.getenv("POSTGRES_PASS")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", 6543))
    DB_SAVE_CHUNKSIZE: int = int(os.getenv("DB_SAVE_CHUNKSIZE", "10000"))

    DB_POOL_SIZE: int = 10
    DB_MAX_OVERFLOW: int = 20
    DB_POOL_TIMEOUT: int = 30
    DB_POOL_RECYCLE: int = 3600

    # Paths
    KAGGLE_INPUT_PATH: str = os.getenv("KAGGLE_INPUT_PATH", "/app/data/creditcard.csv")
    KAGGLE_OUTPUT_PATH: str = os.getenv("KAGGLE_OUTPUT_PATH", "/app/data/processed_creditcard.csv")

    # KYC Loader Settings
    KYC_NUM_RECORDS: int = int(os.getenv("KYC_NUM_RECORDS", "1000"))
    
    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'
        extra = 'ignore'

settings = Settings()
import os

from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_TRANSACTIONS_TOPIC: str = os.getenv("KAFKA_TRANSACTIONS_TOPIC")
    KAFKA_OWNERSHIP_GRAPH_TOPIC: str = os.getenv("KAFKA_OWNERSHIP_GRAPH_TOPIC")
    KAFKA_ENRICHED_TRANSACTIONS_TOPIC: str = os.getenv("KAFKA_ENRICHED_TRANSACTIONS_TOPIC")
    KAFKA_CONSUMER_GROUP_ID: str = os.getenv("KAFKA_CONSUMER_GROUP_ID", "risk_engine_group")
    AUTO_OFFSET_RESET: str = os.getenv("AUTO_OFFSET_RESET", "earliest")

    POSTGRES_DB: str = os.getenv("POSTGRES_DB")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER")
    POSTGRES_PASS: str = os.getenv("POSTGRES_PASS")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", 6543))
    DATABASE_URL: str = os.getenv("DATABASE_URL", f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASS}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")

    KAGGLE_INPUT_PATH: str = os.getenv("KAGGLE_INPUT_PATH")
    KAGGLE_OUTPUT_PATH: str = os.getenv("KAGGLE_OUTPUT_PATH")
    
    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'
        extra = 'ignore'

settings = Settings()
import os

from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    LOG_LEVEL: str = "INFO"

    NEO4J_URI: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER: str = os.getenv("NEO4J_USER")
    NEO4J_PASS: str = os.getenv("NEO4J_PASS")

    POSTGRES_DB: str = os.getenv("POSTGRES_DB")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER")
    POSTGRES_PASS: str = os.getenv("POSTGRES_PASS")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", 5432))
    DB_SAVE_CHUNKSIZE: int = int(os.getenv("DB_SAVE_CHUNKSIZE", "10000"))

    DB_POOL_SIZE: int = 10
    DB_MAX_OVERFLOW: int = 20
    DB_POOL_TIMEOUT: int = 30
    DB_POOL_RECYCLE: int = 3600

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'
        extra = 'ignore'

settings = Settings()
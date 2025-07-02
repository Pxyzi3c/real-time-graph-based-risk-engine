import os

from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASS}@localhost:6543/{POSTGRES_DB}"
    POSTGRES_USER: str = os.getenv("POSTGRES_USER")
    POSTGRES_PASS: str = os.getenv("POSTGRES_PASS")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB")

    DB_POOL_SIZE: int = 10
    DB_MAX_OVERFLOW: int = 20
    DB_POOL_TIMEOUT: int = 30
    DB_POOL_RECYCLE: int = 3600

    API_KEY: str = "default_key"

    KAGGLE_INPUT_PATH: str = os.getenv("KAGGLE_INPUT_PATH")
    KAGGLE_OUTPUT_PATH: str = os.getenv("KAGGLE_OUTPU_PATH")

    OPENCORPORATES_API_KEY: str = os.getenv("OPENCORPORATES_API_KEY")
    OPENCORPORATES_BASE_URL: str = os.getenv("OPENCORPORATES_BASE_URL", "https://api.opencorporates.com/v0.4")
    
    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'
        extra = 'ignore'

settings = Settings()
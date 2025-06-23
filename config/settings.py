import os

from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()

class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql://{POSTGRES_USER}:{POSTGRES_PASS}@postgres_db:5432/{POSTGRES_DB}"
    API_KEY: str = "default_key"
    POSTGRES_USER: str = os.getenv("POSTGRES_USER")
    POSTGRES_PASS: str = os.getenv("POSTGRES_PASS")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB")

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'
        extra = 'ignore'

settings = Settings()
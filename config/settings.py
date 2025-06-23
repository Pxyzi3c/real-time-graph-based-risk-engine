from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str = "postgresql://{POSTGRES_USER}:{POSTGRES_PASS}@postgres_db:5432/{POSTGRES_DB}"
    API_KEY: str = "default_key"

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'
        extra = 'ignore'

settings = Settings()
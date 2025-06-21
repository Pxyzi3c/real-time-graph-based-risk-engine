from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    DATABASE_URL: str
    API_KEY: str = "default_key" # Can have defaults

    model_config = SettingsConfigDict(env_file='.env', extra='ignore')

settings = Settings()
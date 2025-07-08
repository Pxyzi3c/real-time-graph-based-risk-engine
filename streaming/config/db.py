from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
import pandas as pd
import logging 

from config.settings import settings
from config.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        try:
            database_url = settings.DATABASE_URL
            logger.info(f"Creating SQLAlchemy Engine for URL: {database_url.split('@')[-1]}")

            _engine = create_engine(
                database_url,
                poolclass=QueuePool,
                pool_size=settings.DB_POOL_SIZE,
                max_overflow=settings.DB_MAX_OVERFLOW,
                pool_timeout=settings.DB_POOL_TIMEOUT,
                pool_recycle=settings.DB_POOL_RECYCLE,
            )

            with _engine.connect() as connection:
                connection.execute(text("SELECT 1"))
                logger.info("Database connection test successful.")
        except Exception as e:
            logger.critical(f"Failed to create or connect to database engine: {e}", exc_info=True)
            raise
    return _engine
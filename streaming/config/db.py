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

def get_data(table_name: str, batch_size: int = 1000) -> Generator[pd.DataFrame, None, None]:
    """
    Use the engine to get a connection    
    pd.read_sql_query can take an engine directly, which handles connection pooling
    and closing implicitly when used with `chunksize`.
    """
    engine = None
    try:
        engine = get_engine()
        logger.info(f"Attempting to read data from table: {table_name}")

        query = text(f"SELECT * FROM {table_name}")

        for chunk in pd.read_sql_query(query, engine, chunksize=batch_size):
            logger.info(f"Fetched {len(chunk)} records from '{table_name}'.")
            yield chunk
        
        logger.info(f"Finished fetching all data from '{table_name}'.")
    except Exception as e:
        logger.error(f"Error fetching data from PostgreSQL table '{table_name}': {e}", exc_info=True)
        raise
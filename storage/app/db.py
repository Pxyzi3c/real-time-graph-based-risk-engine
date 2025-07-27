import logging
import pandas as pd
import time

from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool
from sqlalchemy.engine import Engine
from config.settings import settings
from config.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

_engine = None

def get_db_engine():
    """Establishes and returns a SQLAlchemy engine for PostgreSQL."""
    global _engine
    if _engine is None:
        try:
            database_url = f"postgresql://{settings.POSTGRES_USER}:{settings.POSTGRES_PASS}@{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}"
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

def save_dataframe_to_db(df: pd.DataFrame, table_name: str, if_exists: str = 'append', index: bool = False, chunksize: int = 1000):
    """
    Saves a Pandas DataFrame to a PostgreSQL table.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        table_name (str): The name of the table to save to.
        if_exists (str): How to behave if the table already exists.
                         Options: 'fail', 'replace', 'append'. Default 'append'.
        index (bool): Write DataFrame index as a column. Default False.
        chunksize (int): Number of rows to write at a time. Default 1000.
    """
    if df.empty:
        logger.warning("DataFrame is empty. No data to save to the table '{table_name}'.")
        return

    engine = get_db_engine()

    try:
        df.to_sql(table_name, engine, if_exists=if_exists, index=index, chunksize=chunksize)
        logger.info(f"DataFrame successfully saved to PostgreSQL table '{table_name}' with {if_exists} mode.")
    except Exception as e:
        logger.error(f"Error saving to table '{table_name}': {e}")
        raise
    finally:
        pass
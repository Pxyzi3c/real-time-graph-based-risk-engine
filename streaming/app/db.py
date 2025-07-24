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

CREATE_ENRICHED_TRANSACTIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS enriched_transactions (
    transaction_id VARCHAR(255) PRIMARY KEY,
    time NUMERIC,
    v1 NUMERIC,
    v2 NUMERIC,
    v3 NUMERIC,
    v4 NUMERIC,
    v5 NUMERIC,
    v6 NUMERIC,
    v7 NUMERIC,
    v8 NUMERIC,
    v9 NUMERIC,
    v10 NUMERIC,
    v11 NUMERIC,
    v12 NUMERIC,
    v13 NUMERIC,
    v14 NUMERIC,
    v15 NUMERIC,
    v16 NUMERIC,
    v17 NUMERIC,
    v18 NUMERIC,
    v19 NUMERIC,
    v20 NUMERIC,
    v21 NUMERIC,
    v22 NUMERIC,
    v23 NUMERIC,
    v24 NUMERIC,
    v25 NUMERIC,
    v26 NUMERIC,
    v27 NUMERIC,
    v28 NUMERIC,
    amount NUMERIC,
    class INTEGER,
    customer_id VARCHAR(255),
    company_number VARCHAR(255),
    kyc_full_name VARCHAR(255),
    kyc_email VARCHAR(255),
    kyc_phone_number VARCHAR(255),
    kyc_address TEXT,
    kyc_country VARCHAR(100),
    kyc_date_of_birth DATE,
    kyc_gender VARCHAR(50),
    kyc_occupation VARCHAR(255),
    kyc_registration_date TIMESTAMP,
    kyc_last_login_ip VARCHAR(50),
    kyc_device_id VARCHAR(255),
    kyc_kyc_status VARCHAR(50),
    kyc_risk_score NUMERIC(5,2),
    kyc_updated_at TIMESTAMP,
    ownership_links_json JSONB
);
"""

def create_table_if_not_exists(sql_statement: str, table_name: str, max_retries: int = 5, retry_delay_seconds: int = 5):
    """
    Attempts to create a table using the provided SQL statement.
    Retries multiple times if the database is not ready.
    """
    engine = get_db_engine()
    
    for attempt in range(max_retries):
        try:
            with engine.connect() as connection:
                connection.execute(text(sql_statement))
                connection.commit()
            logger.info(f"Table '{table_name}' created or already exists.")
            return True
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries}: Failed to create table '{table_name}'. Retrying in {retry_delay_seconds} seconds. Error: {e}")
            time.sleep(retry_delay_seconds)
    
    logger.critical(f"Failed to create table '{table_name}' after {max_retries} attempts. Database might not be fully ready or connection issue persists.")
    raise Exception(f"Failed to create table '{table_name}' after multiple attempts.")

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

def get_dataframe_from_db(table_name: str, columns: list = None) -> pd.DataFrame:
    """
    Reads data from a specified PostgreSQL table into a Pandas DataFrame.

    Args:
        table_name (str): The name of the table to read from.
        columns (list, optional): A list of column names to select. If None, all columns are selected.

    Returns:
        pd.DataFrame: A DataFrame containing the data from the table.
    """
    engine = get_db_engine()
    query = f"SELECT {', '.join(columns) if columns else '*'} FROM {table_name}"
    try:
        df = pd.read_sql(query, engine)
        logger.info(f"Successfully read {len(df)} rows from table '{table_name}'.")
        return df
    except Exception as e:
        logger.error(f"Error reading from table '{table_name}': {e}")
        raise
    finally:
        pass

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

    # if 'transaction_id' in df.columns:
    if table_name == 'enriched_transactions' and 'transaction_id' in df.columns:
        try:
            conn = engine.connect()
            data = df.to_dict(orient='records')

            df_columns_for_sql = [col.lower() for col in df.columns]

            cols = ', '.join([f'"{col}"' if col.lower() != col else col for col in df_columns_for_sql])
            val_placeholders = ', '.join([f":{col}" for col in df_columns_for_sql])

            insert_query = text(f"""
                INSERT INTO {table_name} ({cols})
                VALUES ({val_placeholders})
                ON CONFLICT (transaction_id) DO NOTHING
            """)

            prepared_data = []
            for record in data:
                prepared_record = {k.lower(): v for k, v in record.items()} # Convert keys to lowercase for matching placeholders
                prepared_data.append(prepared_record)

            for i in range(0, len(data), chunksize):
                chunk = data[i:i + chunksize]
                conn.execute(insert_query, chunk)
                logger.debug(f"Inserted/skipped {len(chunk)} rows into {table_name}.")

            conn.commit()
            logger.info(f"DataFrame successfully saved to PostgreSQL table '{table_name}' with ON CONFLICT DO NOTHING.")
        except Exception as e:
            logger.error(f"Failed to save DataFrame to PostgreSQL table '{table_name}' with idempotency: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
            # engine.dispose()

    else:
        try:
            df.to_sql(table_name, engine, if_exists=if_exists, index=index, chunksize=chunksize)
            logger.info(f"DataFrame successfully saved to PostgreSQL table '{table_name}' with {if_exists} mode.")
        except Exception as e:
            logger.error(f"Error saving to table '{table_name}': {e}")
            raise
        finally:
            pass
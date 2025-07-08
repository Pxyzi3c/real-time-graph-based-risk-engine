import logging 
import psycopg2
from config.settings import settings
from config.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=settings.POSTGRES_DB,
            user=settings.POSTGRES_USER,
            password=settings.POSTGRES_PASS,
            host=settings.POSTGRES_HOST,
            port=settings.POSTGRES_PORT
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to the database: {e}")
        return None
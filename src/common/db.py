from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from config.settings import settings

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        database_url = settings.DATABASE_URL

        _engine = create_engine(
            database_url,
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,
            pool_recycle=3600,
        )
    return _engine

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=get_engine())
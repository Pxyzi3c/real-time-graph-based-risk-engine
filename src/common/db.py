from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
from config.settings import settings
from sqlalchemy.engine import URL

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

        # _engine2 = create_engine(URL.create(
        #     drivername='postgresql+psycopg2',
        #     username=settings.POSTGRES_USER,
        #     password=settings.POSTGRES_PASS,
        #     host='postgres_db',
        #     port=5432,
        #     database=settings.POSTGRES_DB
        # ), poolclass=QueuePool, pool_size=10, max_overflow=20, pool_timeout=30, pool_recycle=3600)
        
        # print(f"Engine: {_engine}\nEngine2: {_engine2}")
    return _engine

# get_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=get_engine())
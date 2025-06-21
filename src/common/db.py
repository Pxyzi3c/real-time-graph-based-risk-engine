from sqlalchemy import create_engine
from config.settings import settings

_engine = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(settings.DATABASE_URL)
        # Add connection pooling settings, e.g., pool_size, max_overflow
    return _engine
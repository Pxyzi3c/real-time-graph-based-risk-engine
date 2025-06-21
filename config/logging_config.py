import logging
import os

def setup_logging():
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            # logging.FileHandler("app.log") # Uncomment for file logging
        ]
    )
    # Suppress chatty loggers (e.g., SQLAlchemy)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

# Call this at the start of your application
# from config.logging_config import setup_logging
# setup_logging()
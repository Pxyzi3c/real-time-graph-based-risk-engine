import logging
import json
import pandas as pd
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError
from typing import Dict, Any, Generator

from config.settings import settings
from config.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

KAFKA_SERVER = settings.KAFKA_BOOTSTRAP_SERVERS
TOPIC = settings.TRANSACTIONS_TOPIC
KAGGLE_PROCESSED_DATA = settings.KAGGLE_OUTPUT_PATH

def get_kafka_producer() -> KafkaProducer:
    retries = 5
    delay = 5
    for i in range(retries):
        try:
            logger.info(f"Attempting to connect to Kafka brokers at: {KAFKA_SERVER} (Attempt {i+1}/{retries})")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks='all',
                retries=3,
                linger_ms=50,
                batch_size=16384,
                max_block_ms=30000
            )

            producer.bootstrap_connected()
            logger.info("Kafka producer initialized successfully.")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"No Kafka brokers available at {KAFKA_SERVER}. Retrying in {delay} seconds...")
            time.sleep(delay)
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}", exec_info=True)
            time.sleep(delay)
    raise ConnectionError(f"Could not connect to Kafka brokers at {KAFKA_SERVER} after {retries} attempts.")

df = pd.read_csv(KAGGLE_PROCESSED_DATA)
logger.info(f"Producing {len(df)} records to topic '{TOPIC}'...")

for _, row in df.iterrows():
    producer.send(TOPIC, row.to_dict())

producer.flush()

if __name__ == "__main__":
    producer = get_kafka_producer()
    producer()
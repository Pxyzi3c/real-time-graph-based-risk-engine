import logging
import json
import time
import pandas as pd
from confluent_kafka import Producer, KafkaException
from config.settings import settings
from app.utils import create_kafka_topics, get_kafka_producer_config
from config.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

class KafkaProducer:
    def __init__(self, topic: str, bootstrap_servers: str):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self._initialize_producer()

    def _initialize_producer(self):
        """Initializes the Kafka producer and creates topics if they don't exist."""
        try:
            create_kafka_topics(self.bootstrap_servers, [self.topic])
            self.producer = Producer(get_kafka_producer_config())
            logger.info(f"Kafka Producer initialized for topic: {self.topic}")
        except KafkaException as e:
            logger.error(f"Failed to initialize Kafka Producer: {e}")

    def delivery_report(self, err, msg):
        """Callback function for Kafka message delivery reports."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to topic '{msg.topic()}' [{msg.partition()}] at offset {msg.offset()}")

    def produce_message(self, key: str, value: dict):
        """Produces a single message to the Kafka topic."""
        try:
            self.producer.produce(
                topic=self.topic,
                key=key.encode('utf-8'),
                value=json.dumps(value).encode('utf-8'),
                callback=self.delivery_report
            )
            self.producer.poll(0) # Poll for callbacks immediately
        except KafkaException as e:
            logger.error(f"Kafka production error: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during message production: {e}")
            raise

    def flush(self):
        """Flushes any outstanding messages to the broker."""
        self.producer.flush()
        logger.info(f"Kafka Producer flushed for topic: {self.topic}")

    def __del__(self):
        """Ensures all messages are flushed when the producer object is deleted."""
        if self.producer:
            self.flush()

def push_transactions_to_kafka(df: pd.DataFrame, topic: str, bootstrap_servers: str):
    """
    Pushes transaction data from a Pandas DataFrame to a Kafka topic.
    Each row in the DataFrame is considered a transaction.
    """
    for col in df.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64[ns]']).columns:
        df[col] = df[col].dt.isoformat() if not df[col].isnull().all() else None # Convert to ISO string, handle NaT


    transaction_producer = KafkaProducer(topic, bootstrap_servers)
    logger.info(f"Starting to push {len(df)} transactions to Kafka topic '{topic}'...")

    for index, row in df.iterrows():
        try:
            transaction_key = str(index) 
            transaction_data = row.to_dict()
            transaction_producer.produce_message(transaction_key, transaction_data)
        except Exception as e:
            logger.error(f"Failed to push transaction {index} to Kafka: {e}")
    transaction_producer.flush()
    logger.info(f"Finished pushing transactions to Kafka topic '{topic}'.")


def push_ownership_graph_to_kafka(df: pd.DataFrame, topic: str, bootstrap_servers: str):
    """
    Pushes ownership graph data from a Pandas DataFrame to a Kafka topic.
    Each row represents an ownership link.
    """
    for col in df.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64[ns]']).columns:
        df[col] = df[col].dt.isoformat() if not df[col].isnull().all() else None
    
    ownership_producer = KafkaProducer(topic, bootstrap_servers)
    logger.info(f"Starting to push {len(df)} ownership links to Kafka topic '{topic}'...")

    for index, row in df.iterrows():
        try:
            link_key = f"{row['company_number']}-{row['related_entity_name']}-{index}"
            link_data = row.to_dict()
            ownership_producer.produce_message(link_key, link_data)
            time.sleep(0.005) # Small delay
        except Exception as e:
            logger.error(f"Failed to push ownership link {index} to Kafka: {e}")
    ownership_producer.flush()
    logger.info(f"Finished pushing ownership links to Kafka topic '{topic}'.")
import logging
import json
import time
import pandas as pd
import numpy as np
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
            # create_kafka_topics(self.bootstrap_servers, [self.topic])
            self.producer = Producer(get_kafka_producer_config())
            logger.info(f"Kafka Producer initialized for topic: {self.topic}")
        except KafkaException as e:
            logger.error(f"Failed to initialize Kafka Producer: {e}")
        except Exception as e:
            logger.critical(f"An unexpected critical error occurred during Kafka Producer initialization: {e}", exc_info=True)
            raise

    def delivery_report(self, err, msg):
        """Callback function for Kafka message delivery reports."""
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to topic '{msg.topic()}' [{msg.partition()}] at offset {msg.offset()}")
            pass

    def produce_message(self, key: str, value: dict):
        """Produces a single message to the Kafka topic."""
        try:
            self.producer.produce(
                topic=self.topic,
                key=key.encode('utf-8'),
                value=json.dumps(value).encode('utf-8'),
                callback=self.delivery_report
            )
            self.producer.poll(0)
        except (KafkaException, BufferError) as e:
            logger.error(f"Kafka production failed for key '{key}'. Error: {e}")
            raise
        except TypeError as e:
            logger.error(f"JSON serialization error for key '{key}'. Data: {value}. Error: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during message production for key '{key}': {e}", exc_info=True)
            raise

    def flush(self):
        """
        Flushes any outstanding messages to the broker.
        Handles potential Kafka errors during flush.
        """
        try:
            remaining_messages = self.producer.flush(timeout=10) # Add a timeout
            if remaining_messages > 0:
                logger.warning(f"Producer flush timed out, {remaining_messages} messages still in queue for topic: {self.topic}")
            else:
                logger.info(f"Kafka Producer flushed for topic: {self.topic}")
        except KafkaException as e:
            logger.error(f"Failed to flush Kafka Producer for topic '{self.topic}': {e}", exc_info=True)
            raise # Re-raise to signal a flush problem
        except Exception as e:
            logger.critical(f"An unexpected critical error during Kafka Producer flush: {e}", exc_info=True)
            raise

    def __del__(self):
        """Ensures all messages are flushed when the producer object is deleted."""
        if self.producer:
            try:
                self.producer.flush(timeout=5)
                logger.debug(f"Producer __del__ flushed remaining messages for topic: {self.topic}")
            except Exception as e:
                logger.error(f"Error during producer __del__ flush for topic '{self.topic}': {e}")

def push_transactions_to_kafka(df: pd.DataFrame, topic: str, bootstrap_servers: str):
    """
    Pushes transaction data from a Pandas DataFrame to a Kafka topic.
    Each row in the DataFrame is considered a transaction.
    """
    for col in df.select_dtypes(include=['datetime64[ns, UTC]', 'datetime64[ns]']).columns:
        df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ').replace({np.nan: None})

    try: 
        transaction_producer = KafkaProducer(topic, bootstrap_servers)
    except Exception as e:
        logger.error(f"Failed to initialize Kafka Producer for topic '{topic}': {e}")
        return
    
    logger.info(f"Starting to push {len(df)} transactions to Kafka topic '{topic}'...")

    for index, row in df.iterrows():
        try:
            transaction_key = str(row.get('transaction_id', index)) 
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
        df[col] = df[col].dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ').replace({np.nan: None})
        
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
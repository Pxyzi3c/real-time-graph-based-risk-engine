import logging
import json
import time
import pandas as pd
from confluent_kafka import Producer
from streaming.config.kafka_config import kafka_config
from streaming.config.db import get_db_connection

logger = logging.getLogger(__name__)

class KafkaProducer:
    def __init__(self):
        self.producer = Producer({'bootstrap.servers': kafka_config.BOOTSTRAP_SERVERS})
        logger.info(f"Kafka Producer initialized with bootstrap servers: {kafka_config.BOOTSTRAP_SERVERS}")

    def delivery_report(self, err, msg):
        if err is not None:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.info(f"Message delivered to topic '{msg.topic()}' [{msg.partition()}] at offset {msg.offset()}")
    
    def produce_from_postgresql(self, table_name: str, topic: str, batch_size: int = 1000):
        try:
            conn = get_db_connection()
            if conn:
                logger.info(f"Connected to PostgreSQL. Starting to read data from table: {table_name}")
                query = f"SELECT * FROM {table_name};"
                
                for chunk in pd.read_sql_query(query, conn, chunksize=batch_size):
                    for index, row in chunk.iterrows():
                        key = str(row['id']) if 'id' in row else str(index)
                        value = row.to_json()
                        self.producer.produce(topic, key=key.encode('utf-8'), value=value.encode('utf-8'), callback=self.delivery_report)
                        self.producer.poll(0)
                    logger.info(f"Produced {len(chunk)} messages to topic '{topic}'.")
                    time.sleep(0.1)

                self.producer.flush()
                logger.info(f"Finished producing data from '{table_name}' to topic '{topic}'.")
            else:
                logger.error("Failed to establish database connection.")
        except Exception as e:
            logger.error(f"Error producing from PostgreSQL: {e}")
        finally:
            if conn:
                conn.close()

if __name__ == "__main__":
    from config.logging_config import setup_logging
    setup_logging()

    producer_app = KafkaProducer()

    logger.info(f"Producing data from 'credit_card_fraud' table to topic '{kafka_config.TRANSACTIONS_TOPIC}'...")
    producer_app.produce_from_postgresql("credit_card_fraud", kafka_config.TRANSACTIONS_TOPIC)

    logger.info(f"Producing data from 'company_ownership_links' table to topic '{kafka_config.OWNERSHIP_GRAPH_TOPIC}'...")
    producer_app.produce_from_postgresql("company_ownership_links", kafka_config.OWNERSHIP_GRAPH_TOPIC)
# import time
# import pandas as pd
# import logging
# from streaming.config.kafka_config import get_producer
# from streaming.config.settings import settings
# from streaming.app.utils import get_db_connection, serialize_message, setup_logging, safe_float

# setup_logging()
# logger = logging.getLogger(__name__)

# def delivery_report(err, msg):
#     if err is not None:
#         logger.error(f"Message delivery failed: {err}")
#     else:
#         logger.info(f"Message delivered to topic {msg.topic()} [{msg.parition()}] at offset {msg.offset()}")

# def produce_transactions_from_postgres(topic: str, batch_size: int = 100, delay_seconds: float = 0.1):
#     producer = get_producer()
#     conn = None
#     try:
#         conn = get_db_connection()
#         cursor = conn.cursor()

#         cursor.execute("SELECT COUNT(*) FROM credit_card_fraud;")
#         total_transactions = cursor.fetchone()[0]
#         logger.info(f"Total transactions in PostgreSQL: {total_transactions}")

#         offset = 0
#         while True:
#             query = f"SELECT * FROM credit_card_fraud ORDER BY id ASC OFFSET {offset} LIMIT {batch_size};"
#             df = pd.read_sql(query, conn)

#             if df.empty:
#                 logger.info("Reached end of transactions in PostgreSQL. Restarting from beginning.")
#                 offset = 0
#                 time.sleep(1)
#                 continue

#             for index, row in df.iterrows():
#                 transaction_data = row.to_dict()

#                 transaction_data['Amount'] = safe_float(transaction_data.get('Amount'))
#                 transaction_data['Class'] = int(transaction_data.get('Class', 0))

#                 for key, value in transaction_data.items():
#                     if isinstance(value, (pd.Timestamp, pd.Timedelta)):
#                         transaction_data[key] = str(value)
#                     elif hasattr(value, 'hex'):
#                         transaction_data[key] = value.hex()

#                 producer.produce(topic, key=str(transaction_data['id']).encode('utf-8'),
#                                  value=serialize_message(transaction_data),
#                                  callback=delivery_report)
#                 producer.poll(0)

#             offset += len(df)
#             logger.info(f"Produced {len(df)} transactions. Total produced in this cycle: {offset}")
#             time.sleep(delay_seconds)
    
#     except Exception as e:
#         logger.error(f"Error in producer: {e}")
#     finally:
#         if producer:
#             producer.flush()
#         if conn:
#             conn.close()
#         logger.info("Producer stopped.")

# if __name__ == "__main__":
#     logger.info(f"Starting Kafka producer for topic: {settings.KAFKA_TRANSACTIONS_TOPIC}")
import logging
import json
import time
import pandas as pd
from confluent_kafka import Producer
from config.kafka_config import kafka_config
from config.settings import settings
from config.db import get_engine

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
    
    def produce_from_postgres(self, table_name: str, topic: str, batch_size: int = 100):
        #

if __name__ == "__main__":
    from config.logging_config import setup_logging
    setup_logging()

    producer_app = KafkaProducer()

    logger.info(f"Producing data from 'credit_card_fraud' table to topic '{kafka_config.TRANSACTIONS_TOPIC}'...")
    producer_app.produce_from_postgresql("credit_card_fraud", kafka_config.TRANSACTIONS_TOPIC)

    logger.info(f"Producing data from 'company_ownership_links' table to topic '{kafka_config.OWNERSHIP_GRAPH_TOPIC}'...")
    producer_app.produce_from_postgresql("company_owership_links", kafka_config.OWNERSHIP_GRAPH_TOPIC)
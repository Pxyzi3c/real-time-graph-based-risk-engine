import logging
import json
import time
import hashlib
import pandas as pd

from confluent_kafka import Consumer, KafkaException, Producer
from app.enricher import TransactionEnricher
from app.db import save_dataframe_to_db, create_table_if_not_exists, CREATE_ENRICHED_TRANSACTIONS_TABLE_SQL
from app.utils import get_kafka_consumer_config, get_kafka_producer_config
from config.settings import settings
from config.logging_config import setup_logging
from datetime import datetime

from storage.app.neo4j_loader import GraphLoader
from storage.app.parquet_saver import save_to_parquet

setup_logging()
logger = logging.getLogger(__name__)

class KafkaConsumer:
    def __init__(self, consumer_group_id: str, input_topic: str, output_topic: str, bootstrap_servers: str):
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.bootstrap_servers = bootstrap_servers
        self.consumer = Consumer(get_kafka_consumer_config(consumer_group_id))
        self.producer = Producer(get_kafka_producer_config())
        self.enricher = TransactionEnricher()
        self.consumer.subscribe([self.input_topic, settings.KAFKA_OWNERSHIP_GRAPH_TOPIC])
        logger.info(f"Kafka Consumer initialized for input topic: {self.input_topic}, output topic: {self.output_topic}")
        
        logger.info("Ensuring 'enriched_transactions' table exists...")
        create_table_if_not_exists(CREATE_ENRICHED_TRANSACTIONS_TABLE_SQL, 'enriched_transactions')
        
        self.running = True
        self.batch = []
        self.batch_size = 100
        self.flush_interval = 5
        self.last_flush_time = time.time()

    def _delivery_report(self, err, msg):
        """Callback function for Kafka message delivery reports for the producer."""
        if err is not None:
            logger.error(f"Message delivery failed to enriched topic: {err}")
        else:
            logger.debug(f"Message delivered to enriched topic '{msg.topic()}' [{msg.partition()}] at offset {msg.offset()}")

    def _process_message(self, msg_value: bytes) -> dict:
        """Processes and enriches a single transaction message."""
        try:
            transaction_data = json.loads(msg_value.decode('utf-8'))
            enriched_transaction = self.enricher.enrich_transaction(transaction_data)
            
            def convert_non_serializable(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                elif isinstance(obj, pd.Timestamp):
                    return obj.isoformat()
                elif isinstance(obj, dict):
                    return {k: convert_non_serializable(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [convert_non_serializable(elem) for elem in obj]
                return obj
            
            return convert_non_serializable(enriched_transaction)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON from message: {e}")
            return {} # Return empty dict or handle error appropriately
        except Exception as e:
            logger.error(f"Error during transaction enrichment: {e}")
            return {}

    def _handle_ownership_graph_message(self, msg_value: bytes):
        """Handles updates to the ownership graph data."""
        try:
            # For now, we simply re-load the ownership graph in the enricher.
            # In a production system, this might involve an in-memory update
            # or a more sophisticated state management for the enricher.
            self.enricher._load_ownership_graph_data() 
            logger.info("Ownership graph data reloaded due to update.")
        except Exception as e:
            logger.error(f"Failed to update ownership graph from Kafka message: {e}")

    def start_consuming(self):
        """Starts consuming messages from Kafka topics."""
        logger.info(f"Starting to consume messages from topics: {self.input_topic}, {settings.KAFKA_OWNERSHIP_GRAPH_TOPIC}")

        while self.running:
            msg = self.consumer.poll(timeout=1.0)

            if msg is None:
                if self.batch and (time.time() - self.last_flush_time >= self.flush_interval):
                    self._process_and_save_batch(self.batch)
                    self.batch = []
                    self.last_flush_time = time.time()
                continue
            if msg.error():
                if msg.error().code() == -191:
                    logger.debug(f"End of partition reached for {msg.topic()} [{msg.partition()}]")
                else:
                    logger.error(f"Kafka consumer error: {msg.error()}")
                continue

            if msg.topic() == self.input_topic:
                enriched_data = self._process_message(msg.value())
                if enriched_data:
                    self.batch.append(enriched_data)
                    self.producer.produce(
                        topic=self.output_topic,
                        key=msg.key(),
                        value=json.dumps(enriched_data).encode('utf-8'),
                        callback=self._delivery_report
                    )
            elif msg.topic() == settings.KAFKA_OWNERSHIP_GRAPH_TOPIC:
                self._handle_ownership_graph_message(msg.value())
            
            if len(self.batch) >= self.batch_size or (time.time() - self.last_flush_time >= self.flush_interval and self.batch):
                self._process_and_save_batch(self.batch)
                self.batch = []
                self.last_flush_time = time.time()

            self.producer.poll(0) # Poll producer for delivery reports

        # Flush any remaining messages in the batch before shutting down
        if self.batch:
            self._process_and_save_batch(self.batch)
        self.producer.flush()
        self.consumer.close()
        logger.info("Kafka Consumer stopped.")

    def _process_and_save_batch(self, batch: list):
        """Processes a batch of enriched transactions and saves them to PostgreSQL."""
        if not batch:
            return
        
        try:
            # Flatten the nested dictionaries (kyc_info, ownership_links) for PostgreSQL
            processed_batch = []
            for item in batch:
                flat_item = item.copy()
                kyc_info = flat_item.pop('kyc_info', {})
                ownership_links = flat_item.pop('ownership_links', [])

                # Prefix KYC keys to avoid collision and flatten
                for k, v in kyc_info.items():
                    flat_item[f'kyc_{k}'] = str(v) if not isinstance(v, (str, int, float, bool)) else v
                
                flat_item['ownership_links_json'] = json.dumps(ownership_links)
                
                processed_batch.append(flat_item)

                # TO-DO: Arrange parquet and neo4j processes in this function. Not sure if this is the right place but this process should also be in this function
                # neo4j_loader.insert_transaction({
                #     "customer_id": item.get("customer_id"),
                #     "transaction_id": item.get("transaction_id"),
                #     "amount": float(item.get("amount", 0)),
                #     "risk_score": float(item.get("kyc_risk_score", 0))
                # })
                
                # save_to_parquet(df, f"enriched_batch_{int(time.time())}.parquet")

            df = pd.DataFrame(processed_batch)
            
            # Select relevant columns for the enriched transactions table
            # You'll need to define what columns you expect in the final enriched table
            # Based on Kaggle data, we have Time, V1-V28, Amount, Class
            # Plus our added customer_id, kyc_*, ownership_links_json
            # Need to align this with your actual enriched schema
            # For now, let's save all flattened columns.
            column_mapping = {
                'Time': 'time',
                'Amount': 'amount',
                'Class': 'class',
                'transaction_id': 'transaction_id',
                'customer_id': 'customer_id',
                'company_number': 'company_number', # Added for consistency, even if sometimes null
                'ownership_links_json': 'ownership_links_json'
            }

            for i in range(1, 29):
                column_mapping[f'V{i}'] = f'v{i}'
            
            if processed_batch:
                sample_item = processed_batch[0]
                for key in sample_item.keys():
                    if key.startswith('kyc_'):
                        column_mapping[key] = key

            df.rename(columns=column_mapping, inplace=True)

            save_dataframe_to_db(df, 'enriched_transactions', if_exists='append')
            logger.info(f"Successfully saved {len(batch)} enriched transactions to PostgreSQL.")
        except Exception as e:
            logger.error(f"Failed to process and save batch of enriched transactions: {e}")

    def stop_consuming(self):
        """Stops the Kafka consumer."""
        self.running = False
        logger.info("Stopping Kafka Consumer...")
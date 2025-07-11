import logging
import json
import time
import pandas as pd
from confluent_kafka import Consumer, KafkaException, Producer
from config.settings import settings
from config.logging_config import setup_logging
from app.db import save_dataframe_to_db

setup_logging()
logger = logging.getLogger(__name__)

ownership_graph_cache = {}
kyc_data_cache = {}

class KafkaConsumerService:
    def __init__(self, topics: list, group_id: str):
        self.consumer = Consumer({
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': group_id,
            'auto.offset.reset': 'earliest', # Start consuming from the beginning of the topic if no offset is committed
            'enable.auto.commit': False # Manual commit for precise control
        })
        self.topics = topics
        self.consumer.subscribe(self.topics)
        logger.info(f"Kafka Consumer initialized for topics: {', '.join(topics)} with group ID: {group_id}")

    def poll_message(self, timeout: float = 1.0):
        """Polls for a single message."""
        return self.consumer.poll(timeout)

    def commit_offsets(self, message):
        """Commits the offset of a processed message."""
        self.consumer.commit(message=message, asynchronous=True)
        logger.debug(f"Committed offset for topic '{message.topic()}' partition {message.partition()} offset {message.offset()}")

    def close(self):
        """Closes the consumer."""
        self.consumer.close()
        logger.info("Kafka Consumer closed.")

class KafkaStreamsProcessor:
    def __init__(self):
        self.transactions_consumer = KafkaConsumerService(
            topics=[settings.KAFKA_TRANSACTIONS_TOPIC],
            group_id=f"{settings.KAFKA_CONSUMER_GROUP_ID}-transactions"
        )
        self.ownership_consumer = KafkaConsumerService(
            topics=[settings.OWNERSHIP_GRAPH_TOPIC],
            group_id=f"{settings.KAFKA_CONSUMER_GROUP_ID}-ownership"
        )
        self.producer = Producer({
            'bootstrap.servers': settings.KAFKA_BOOTSTRAP_SERVERS,
            'client.id': 'kafka-enrichment-producer'
        })
        logger.info("Kafka Streams Processor initialized.")

    def delivery_report(self, err, msg):
        """Callback function for successful or failed message delivery for enriched data."""
        if err is not None:
            logger.error(f"Enriched message delivery failed: {err}")
        else:
            logger.debug(f"Enriched message delivered to topic '{msg.topic()}' [{msg.partition()}] at offset {msg.offset()}")
            pass

    def produce_enriched_message(self, value: dict):
        """Produces an enriched message to the enriched transactions topic."""
        try:
            self.producer.produce(
                settings.KAFKA_ENRICHED_TRANSACTIONS_TOPIC,
                key=str(value.get('id', 'unknown')).encode('utf-8'), # Use transaction 'id' as key
                value=json.dumps(value).encode('utf-8'),
                callback=self.delivery_report
            )
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"Failed to produce enriched message: {e}")

    def load_ownership_data_to_cache(self, msg_value: dict):
        """Loads ownership data into an in-memory cache."""
        company_number = msg_value.get('company_number')
        related_entity_name = msg_value.get('related_entity_name')
        if company_number and related_entity_name:
            # This is a simplification; a real graph would need a more sophisticated lookup.
            if company_number not in ownership_graph_cache:
                ownership_graph_cache[company_number] = []
            ownership_graph_cache[company_number].append(msg_value)
            # Also store by related entity name if it's a company
            if msg_value.get('related_entity_type') == 'company':
                if related_entity_name not in ownership_graph_cache:
                    ownership_graph_cache[related_entity_name] = []
                ownership_graph_cache[related_entity_name].append(msg_value)
            logger.debug(f"Cached ownership link for {company_number} -> {related_entity_name}")
        else:
            logger.warning(f"Invalid ownership data received for caching: {msg_value}")

    def enrich_transaction(self, transaction: dict) -> dict:
        """
        Enriches a transaction with ownership and (placeholder) KYC data.

        Args:
            transaction (dict): The raw transaction data.

        Returns:
            dict: The enriched transaction data.
        """
        enriched_transaction = transaction.copy()

        # Placeholder for KYC enrichment
        # In a real scenario, this would involve looking up customer_id/account_id
        # in kyc_data_cache or a separate service.
        # For now, we'll just add a placeholder.
        enriched_transaction['kyc_status'] = "not_available"
        enriched_transaction['customer_geo'] = "unknown" # Placeholder
        # TODO: Implement actual KYC lookup when data is available.

        # Ownership graph enrichment
        # This is a simplified lookup. In a real AML system, this would involve
        # complex graph traversal to find ultimate beneficial owners or related entities.
        # For this project, we'll demonstrate a basic direct lookup.
        # Assuming transaction might have a 'merchant_company_number' or 'recipient_company_number'
        # For the Kaggle data, we don't have direct company links.
        # Let's assume we derive a 'related_company_id' from transaction data or
        # simulate it for demonstration purposes.

        # For demonstration: let's assume a 'V_dummy_company_id' column in the Kaggle data
        # maps to a synthetic company number. This will need to be added to the Kaggle data
        # during ingestion or simulated here.
        # Since we don't have a direct link from Kaggle transaction to a company,
        # we'll simulate a lookup based on a random "V" feature if it exists,
        # or just add an empty list.

        enriched_transaction['related_ownership_links'] = []
        
        # Example: Try to link based on some transaction attribute that *could* map to a company
        # As Kaggle data has V1-V28, let's pick one (e.g., V17) and map it to a company number
        # This is a strong assumption for demonstration purposes.
        # In a real scenario, we'd have `merchant_id` or `receiving_bank_id` that maps to a company.
        # We need a more robust way to link transactions to entities in the ownership graph.
        
        # FOR DEMONSTRATION PURPOSES: Let's randomly pick a 'company_number' from our cache
        # and attach some ownership links to a percentage of transactions.
        if ownership_graph_cache and transaction.get('id') % 10 == 0: # Enrich every 10th transaction
            random_company_number = list(ownership_graph_cache.keys())[transaction.get('id') % len(ownership_graph_cache)]
            enriched_transaction['linked_company_id'] = random_company_number
            enriched_transaction['related_ownership_links'] = ownership_graph_cache.get(random_company_number, [])
            logger.debug(f"Transaction ID {transaction.get('id')} enriched with company {random_company_number}")
        else:
             enriched_transaction['linked_company_id'] = None
             enriched_transaction['related_ownership_links'] = []

        return enriched_transaction

    def process_messages(self):
        """
        Main loop for consuming and processing Kafka messages.
        Continuously polls for messages from both transaction and ownership topics.
        """
        logger.info("Starting Kafka Streams Processor main loop...")
        running = True
        try:
            while running:
                # Poll for ownership graph updates
                msg_ownership = self.ownership_consumer.poll_message(timeout=0.1)
                if msg_ownership is not None:
                    if msg_ownership.error():
                        if msg_ownership.error().code() == KafkaException.PARTITION_EOF:
                            # End of partition event - not an error
                            continue
                        else:
                            logger.error(f"Ownership consumer error: {msg_ownership.error()}")
                            running = False
                    else:
                        try:
                            ownership_data = json.loads(msg_ownership.value().decode('utf-8'))
                            self.load_ownership_data_to_cache(ownership_data)
                            self.ownership_consumer.commit_offsets(msg_ownership)
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to decode ownership JSON: {e} - Message: {msg_ownership.value().decode('utf-8', errors='ignore')}")
                        except Exception as e:
                            logger.error(f"Error processing ownership message: {e}")
                            self.ownership_consumer.commit_offsets(msg_ownership) # Commit even on error to avoid reprocessing bad messages
                
                # Poll for transaction updates
                msg_transaction = self.transactions_consumer.poll_message(timeout=0.5) # Longer timeout for transactions
                if msg_transaction is not None:
                    if msg_transaction.error():
                        if msg_transaction.error().code() == KafkaException.PARTITION_EOF:
                            # End of partition event - not an error
                            continue
                        else:
                            logger.error(f"Transaction consumer error: {msg_transaction.error()}")
                            running = False
                    else:
                        try:
                            transaction_data = json.loads(msg_transaction.value().decode('utf-8'))
                            
                            # Perform enrichment
                            enriched_transaction = self.enrich_transaction(transaction_data)
                            
                            # Produce enriched message to new topic
                            self.produce_enriched_message(enriched_transaction)
                            
                            self.transactions_consumer.commit_offsets(msg_transaction)
                            
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to decode transaction JSON: {e} - Message: {msg_transaction.value().decode('utf-8', errors='ignore')}")
                        except Exception as e:
                            logger.error(f"Error processing transaction message: {e}")
                            self.transactions_consumer.commit_offsets(msg_transaction) # Commit even on error

                # Flush producer periodically
                self.producer.poll(0) # Poll for delivery reports
                time.sleep(0.01) # Small delay to prevent busy-waiting

        except KeyboardInterrupt:
            logger.info("Stopping Kafka Streams Processor due to KeyboardInterrupt.")
        except Exception as e:
            logger.critical(f"Unhandled error in Kafka Streams Processor: {e}", exc_info=True)
        finally:
            self.transactions_consumer.close()
            self.ownership_consumer.close()
            self.producer.flush(10) # Final flush
            logger.info("Kafka Streams Processor gracefully shut down.")

def consume_and_save_enriched_transactions():
    """
    Consumes enriched transactions from Kafka and saves them to PostgreSQL.
    This acts as the final sink for our streaming layer.
    """
    logger.info(f"Starting enriched transaction consumer for topic '{settings.KAFKA_ENRICHED_TRANSACTIONS_TOPIC}' and saving to PostgreSQL.")
    consumer_service = KafkaConsumerService(
        topics=[settings.KAFKA_ENRICHED_TRANSACTIONS_TOPIC],
        group_id=f"{settings.KAFKA_CONSUMER_GROUP_ID}-enriched-sink"
    )
    
    enriched_transactions_batch = []
    batch_size = 100
    last_save_time = time.time()
    save_interval_seconds = 5

    running = True
    try:
        while running:
            msg = consumer_service.poll_message(timeout=1.0)
            if msg is None:
                # If no message for a while, check if it's time to save existing batch
                if len(enriched_transactions_batch) > 0 and (time.time() - last_save_time) > save_interval_seconds:
                    logger.info(f"Timeout reached, saving {len(enriched_transactions_batch)} enriched transactions to DB.")
                    save_dataframe_to_db(pd.DataFrame(enriched_transactions_batch), 'enriched_transactions', if_exists='append')
                    enriched_transactions_batch = []
                    last_save_time = time.time()
                continue
            
            if msg.error():
                if msg.error().code() == KafkaException.PARTITION_EOF:
                    # End of partition event - not an error
                    continue
                else:
                    logger.error(f"Enriched consumer error: {msg.error()}")
                    running = False
            else:
                try:
                    enriched_data = json.loads(msg.value().decode('utf-8'))
                    enriched_transactions_batch.append(enriched_data)

                    if len(enriched_transactions_batch) >= batch_size:
                        logger.info(f"Batch size reached, saving {len(enriched_transactions_batch)} enriched transactions to DB.")
                        save_dataframe_to_db(pd.DataFrame(enriched_transactions_batch), 'enriched_transactions', if_exists='append')
                        enriched_transactions_batch = []
                        last_save_time = time.time()
                    
                    consumer_service.commit_offsets(msg)

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode enriched JSON: {e} - Message: {msg.value().decode('utf-8', errors='ignore')}")
                except Exception as e:
                    logger.error(f"Error processing enriched message: {e}")
                    consumer_service.commit_offsets(msg) # Commit even on error to avoid reprocessing bad messages

    except KeyboardInterrupt:
        logger.info("Stopping enriched transaction consumer due to KeyboardInterrupt.")
    except Exception as e:
        logger.critical(f"Unhandled error in enriched transaction consumer: {e}", exc_info=True)
    finally:
        if len(enriched_transactions_batch) > 0:
            logger.info(f"Final flush: saving {len(enriched_transactions_batch)} remaining enriched transactions to DB.")
            save_dataframe_to_db(pd.DataFrame(enriched_transactions_batch), 'enriched_transactions', if_exists='append')
        consumer_service.close()
        logger.info("Enriched transaction consumer gracefully shut down.")

if __name__ == '__main__':
    logger.info("Kafka Consumer module. Run specific functions from main.py or testing.")
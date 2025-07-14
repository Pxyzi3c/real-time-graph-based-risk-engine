import logging
import time
import argparse
from app.db import get_dataframe_from_db
from app.producer import push_transactions_to_kafka, push_ownership_graph_to_kafka
from app.consumer import KafkaConsumer
from app.kyc_loader import generate_and_save_kyc_data
from config.settings import settings
from config.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def run_producer_mode():
    """
    Runs the producer functionality to push data from PostgreSQL to Kafka.
    """
    logger.info("Starting streaming producer mode...")

    # Load Kaggle data from PostgreSQL
    try:
        transactions_df = get_dataframe_from_db('credit_card_fraud')
        logger.info(f"Loaded {len(transactions_df)} transactions from PostgreSQL.")
        push_transactions_to_kafka(
            transactions_df, 
            settings.KAFKA_TRANSACTIONS_TOPIC, 
            settings.KAFKA_BROKER_ADDRESS
        )
    except Exception as e:
        logger.error(f"Failed to load/push transactions: {e}")
        # Decide on error handling: exit, retry, etc. For now, we'll log and continue if possible.

    # Load synthetic ownership data from PostgreSQL
    try:
        ownership_df = get_dataframe_from_db('company_ownership_links')
        logger.info(f"Loaded {len(ownership_df)} ownership links from PostgreSQL.")
        push_ownership_graph_to_kafka(
            ownership_df, 
            settings.KAFKA_OWNERSHIP_GRAPH_TOPIC, 
            settings.KAFKA_BROKER_ADDRESS
        )
    except Exception as e:
        logger.error(f"Failed to load/push ownership links: {e}")

    logger.info("Producer mode finished pushing initial data. Keeping producer alive for potential future pushes...")
    # In a real-time scenario, the producer would continuously monitor a source
    # For this simulation, we will keep it alive for a short period or until manually stopped.
    while True:
        time.sleep(60) # Keep the producer container alive

def run_consumer_mode():
    """
    Runs the consumer functionality to process and enrich data from Kafka.
    """
    logger.info("Starting streaming consumer mode...")
    
    # Generate and save fake KYC data
    logger.info("Generating and saving fake KYC data...")
    generate_and_save_kyc_data(settings.KYC_NUM_RECORDS)
    logger.info("Fake KYC data generated and saved.")

    consumer = KafkaConsumer(
        consumer_group_id=settings.KAFKA_CONSUMER_GROUP_ID,
        input_topic=settings.KAFKA_TRANSACTIONS_TOPIC,
        output_topic=settings.KAFKA_ENRICHED_TRANSACTIONS_TOPIC,
        bootstrap_servers=settings.KAFKA_BROKER_ADDRESS
    )
    consumer.start_consuming()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Streaming Layer for AML Risk Engine")
    parser.add_argument(
        "--mode",
        choices=["producer", "consumer"],
        required=True,
        help="Specify the mode for the streaming service: 'producer' to push data to Kafka, 'consumer' to consume and enrich data."
    )
    args = parser.parse_args()

    if args.mode == "producer":
        run_producer_mode()
    elif args.mode == "consumer":
        run_consumer_mode()
    else:
        logger.error("Invalid mode specified. Please choose 'producer' or 'consumer'.")
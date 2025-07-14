import logging
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException, KafkaError
from config.settings import settings

logger = logging.getLogger(__name__)

def create_kafka_topics(bootstrap_servers: str, topics: list[str], num_partitions: int = 1, replication_factor: int = 1):
    """
    Creates multiple Kafka topics if they don't exist.
    Handles 'topic already exists' gracefully.

    Args:
        bootstrap_servers (str): Kafka broker address (e.g., 'localhost:9092').
        topics (list[str]): A list of topic names to create.
        num_partitions (int): Number of partitions for new topics.
        replication_factor (int): Replication factor for new topics.

    Raises:
        KafkaException: If there's a critical error connecting to Kafka
                        or a non-recoverable error during topic creation.
    """
    admin_client = None
    try:
        admin_config = {'bootstrap.servers': bootstrap_servers}
        admin_client = AdminClient(admin_config)
        logger.info(f"Connected to Kafka AdminClient at {bootstrap_servers}")

        new_topics = [
            NewTopic(topic, num_partitions=num_partitions, replication_factor=replication_factor)
            for topic in topics
        ]

        futures = admin_client.create_topics(new_topics, request_timeout=30.0)

        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Kafka topic '{topic}' created successfully (or already existed with same config).")
            except KafkaException as e:
                if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    logger.info(f"Kafka topic '{topic}' already exists. Skipping creation.")
                elif e.args[0].code() == KafkaError.TOPIC_AUTHORIZATION_FAILED:
                    logger.error(f"Authorization failed for topic '{topic}': {e.args[0].str()}", exc_info=True)
                    raise
                else:
                    logger.error(f"Failed to create topic '{topic}': {e.args[0].str()}", exc_info=True)
                    raise
            except Exception as e:
                logger.critical(f"An unexpected error occurred while creating topic '{topic}': {e}", exc_info=True)
                raise

    except KafkaException as e:
        logger.critical(f"Kafka AdminClient initialization or connection failed to '{bootstrap_servers}': {e}", exc_info=True)
        raise
    except Exception as e:
        logger.critical(f"An unexpected error occurred during Kafka topic creation process: {e}", exc_info=True)
        raise
    finally:
        pass

def get_kafka_producer_config():
    return {
        'bootstrap.servers': settings.KAFKA_BROKER_ADDRESS,
        'client.id': 'python-producer',
        'acks': 'all',
        'retries': 3,
        'linger.ms': 100,
        'compression.type': 'snappy'
    }

def get_kafka_consumer_config(group_id: str):
    return {
        'bootstrap.servers': settings.KAFKA_BROKER_ADDRESS,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 5000,
        'max.poll.interval.ms': 300000,
        'session.timeout.ms': 45000
    }
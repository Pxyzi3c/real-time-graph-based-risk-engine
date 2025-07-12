import logging
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException, KafkaError
from config.settings import settings

logger = logging.getLogger(__name__)

def create_kafka_topics(broker_address: str, topics: list):
    """
    Creates Kafka topics if they do not already exist.

    Args:
        broker_address (str): The Kafka broker address (e.g., 'kafka:9092').
        topics (list): A list of topic names to create.
    """
    admin_client = AdminClient({'bootstrap.servers': broker_address})

    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics]

    futures = admin_client.create_topics(new_topics, request_timeout=15.0)

    for topic, future in futures.items():
        try:
            future.result()
            logger.info(f"Topic '{topic}' created successfully or already exists.")
        except KafkaException as e:
            if e.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                logger.warning(f"Topic '{topic}' already exists. Skipping creation.")
            else:
                logger.error(f"Failed to create topic '{topic}': {e}")
                raise 

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
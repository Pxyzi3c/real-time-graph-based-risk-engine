# from confluent_kafka import Producer, Consumer
# from streaming.config.settings import KAFKA_BOOTSTRAP_SERVERS

# def get_producer_config():
#     return {
#         'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
#         'acks': 'all',
#     }

# def get_consumer_config(group_id: str):
#     return {
#         'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
#         'group.id': group_id,
#         'auto.offset.reset': 'earliest',
#         'enable.auto.commit': False
#     }

# def get_producer():
#     return Producer(get_producer_config())

# def get_consumer(group_id: str):
#     return Consumer(get_consumer_config(group_id))
from dataclasses import dataclass
from config.settings import settings

@dataclass
class KafkaConfig:
    BOOSTRAP_SERVERS: str = settings.KAFKA_BOOTSTRAP_SERVERS
    TRANSACTIONS_TOPIC: str = settings.KAFKA_TRANSACTIONS_TOPIC
    OWNERSHIP_GRAPH_TOPIC: str = settings.KAFKA_OWNERSHIP_GRAPH_TOPIC
    ENRICHED_TRANSACTIONS_TOPIC: str = settings.KAFKA_ENRICHED_TRANSACTIONS_TOPIC
    GROUP_ID: str = settings.KAFKA_CONSUMER_GROUP_ID
    AUTO_OFFSET_RESET: str = settings.AUTO_OFFSET_RESET

kafka_config = KafkaConfig()
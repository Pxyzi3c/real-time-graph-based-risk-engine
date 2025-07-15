from dataclasses import dataclass
from config.settings import settings

@dataclass
class KafkaConfig:
    BOOTSTRAP_SERVERS: str = settings.KAFKA_BOOTSTRAP_SERVERS
    TRANSACTIONS_TOPIC: str = settings.KAFKA_TRANSACTIONS_TOPIC
    OWNERSHIP_GRAPH_TOPIC: str = settings.KAFKA_OWNERSHIP_GRAPH_TOPIC
    ENRICHED_TRANSACTIONS_TOPIC: str = settings.KAFKA_ENRICHED_TRANSACTIONS_TOPIC
    GROUP_ID: str = settings.KAFKA_CONSUMER_GROUP_ID
    AUTO_OFFSET_RESET: str = settings.KAFKA_AUTO_OFFSET_RESET

kafka_config = KafkaConfig()
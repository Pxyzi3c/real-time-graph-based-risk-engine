from confluent_kafka import Producer, Consumer
from streaming.config.settings import KAFKA_BOOTSTRAP_SERVERS

def get_producer_config():
    return {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'acks': 'all',
    }

def get_consumer_config(group_id: str):
    return {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }

def get_producer():
    return Producer(get_producer_config())

def get_consumer(group_id: str):
    return Consumer(get_consumer_config(group_id))
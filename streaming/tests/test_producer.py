import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
import json
from confluent_kafka import KafkaException
from app.producer import KafkaProducer, push_transactions_to_kafka, push_ownership_graph_to_kafka
from config.settings import settings

@pytest.fixture
def mock_kafka_producer_instance():
    with patch('app.producer.Producer') as MockConfluentProducer:
        mock_producer = MockConfluentProducer.return_value
        mock_producer.produce.return_value = None
        mock_producer.flush.return_value = None
        yield mock_producer

@pytest.fixture(autouse=True)
def mock_create_kafka_topics():
    with patch('app.utils.create_kafka_topics') as mock_create_topics:
        mock_create_topics.return_value = None
        yield mock_create_topics

@pytest.fixture
def mock_get_dataframe_from_db():
    with patch('app.db.get_dataframe_from_db') as mock_get_df:
        mock_get_df.side_effect = lambda table_name: {
            'company_ownership_links': pd.DataFrame({
                'company_number': ['CO1', 'CO2', 'CO3'],
                'company_name': ['A', 'B', 'C'],
                'related_entity_name': ['X', 'Y', 'Z'],
                'relationship_type': ['owner', 'director', 'shareholder']
            })
        }.get(table_name, pd.DataFrame())
        yield mock_get_df

def test_kafka_producer_initialization(mock_kafka_producer_instance, mock_create_kafka_topics):
    """Test if KafkaProducer initializes correctly and calls topic creation."""
    producer = KafkaProducer(topic="test_topic", bootstrap_servers="localhost:9092")
    assert producer.producer is not None
    mock_create_kafka_topics.assert_called_once_with(
        "localhost:9092", 
        [settings.KAFKA_TRANSACTIONS_TOPIC, 
         settings.KAFKA_OWNERSHIP_GRAPH_TOPIC, 
         settings.KAFKA_ENRICHED_TRANSACTIONS_TOPIC]
    )

def test_produce_message_success(mock_kafka_producer_instance):
    """Test successful message production."""
    producer = KafkaProducer(topic="test_topic", bootstrap_servers="localhost:9092")
    key = "test_key"
    value = {"data": "test_value"}
    producer.produce_message(key, value)
    
    mock_kafka_producer_instance.produce.assert_called_once()
    args, kwargs = mock_kafka_producer_instance.produce.call_args
    assert kwargs['topic'] == "test_topic"
    assert kwargs['key'] == key.encode('utf-8')
    assert json.loads(kwargs['value'].decode('utf-8')) == value

def test_produce_message_kafka_exception(mock_kafka_producer_instance):
    """Test error handling when Kafka production fails."""
    producer = KafkaProducer(topic="test_topic", bootstrap_servers="localhost:9092")
    mock_kafka_producer_instance.produce.side_effect = KafkaException("Kafka error")
    
    with pytest.raises(KafkaException):
        producer.produce_message("key", {"data": "value"})

def test_push_transactions_to_kafka(mock_kafka_producer_instance, mock_get_dataframe_from_db):
    """Test pushing transactions to Kafka."""
    test_df = pd.DataFrame([
        {"Time": 1.0, "Amount": 10.0, "V1": 1.0, "V2": 2.0, "transaction_id": "tx1"},
        {"Time": 2.0, "Amount": 20.0, "V1": 3.0, "V2": 4.0, "transaction_id": "tx2"}
    ])
    
    push_transactions_to_kafka(test_df, settings.KAFKA_TRANSACTIONS_TOPIC, "localhost:9092")
    
    assert mock_kafka_producer_instance.produce.call_count == len(test_df)
    args, kwargs = mock_kafka_producer_instance.produce.call_args_list[0]
    produced_value = json.loads(kwargs['value'].decode('utf-8'))
    assert 'company_number' in produced_value
    assert produced_value['company_number'] in ['CO1', 'CO2', 'CO3']
    
    mock_kafka_producer_instance.flush.assert_called_once()

def test_push_ownership_graph_to_kafka(mock_kafka_producer_instance):
    """Test pushing ownership graph data to Kafka."""
    test_df = pd.DataFrame([
        {"company_number": "C1", "related_entity_name": "E1"},
        {"company_number": "C2", "related_entity_name": "E2"}
    ])
    
    push_ownership_graph_to_kafka(test_df, settings.KAFKA_OWNERSHIP_GRAPH_TOPIC, "localhost:9092")
    
    assert mock_kafka_producer_instance.produce.call_count == len(test_df)
    mock_kafka_producer_instance.flush.assert_called_once()
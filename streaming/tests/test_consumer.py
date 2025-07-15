import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import json
import pandas as pd
from unittest.mock import MagicMock, patch, call
from confluent_kafka import Consumer, Producer, KafkaException
from confluent_kafka import Message
from app.consumer import KafkaConsumer
from app.enricher import TransactionEnricher
from app.db import save_dataframe_to_db
from config.settings import settings

@pytest.fixture
def mock_kafka_consumer():
    with patch('app.consumer.Consumer') as MockConfluentConsumer:
        mock_consumer_instance = MockConfluentConsumer.return_value
        yield mock_consumer_instance

@pytest.fixture
def mock_kafka_producer():
    with patch('app.consumer.Producer') as MockConfluentProducer:
        mock_producer_instance = MockConfluentProducer.return_value
        mock_producer_instance.produce.return_value = None
        mock_producer_instance.flush.return_value = None
        yield mock_producer_instance

@pytest.fixture
def mock_enricher():
    with patch('app.consumer.TransactionEnricher') as MockEnricher:
        enricher_instance = MockEnricher.return_value
        enricher_instance.enrich_transaction.side_effect = lambda tx: {
            **tx, 
            'kyc_info': {'name': 'Mock KYC'}, 
            'ownership_links': [{'comp': 'Mock Comp'}]
        }
        enricher_instance._load_ownership_graph_data.return_value = pd.DataFrame()
        yield enricher_instance

@pytest.fixture
def mock_save_dataframe_to_db():
    with patch('app.db.save_dataframe_to_db') as mock_save_df:
        mock_save_df.return_value = None
        yield mock_save_df

def create_mock_message(topic, key, value, error=None):
    mock_msg = MagicMock(spec=Message)
    mock_msg.topic.return_value = topic
    mock_msg.key.return_value = key.encode('utf-8') if key else None
    mock_msg.value.return_value = json.dumps(value).encode('utf-8')
    mock_msg.error.return_value = error
    mock_msg.partition.return_value = 0
    mock_msg.offset.return_value = 1
    return mock_msg

def test_kafka_consumer_initialization(mock_kafka_consumer, mock_kafka_producer, mock_enricher):
    """Test if KafkaConsumer initializes correctly."""
    consumer_app = KafkaConsumer(
        consumer_group_id="test_group",
        input_topic="transactions",
        output_topic="enriched_transactions",
        bootstrap_servers="localhost:9092"
    )
    mock_kafka_consumer.assert_called_once()
    mock_kafka_producer.assert_called_once()
    mock_enricher.assert_called_once()
    mock_kafka_consumer.return_value.subscribe.assert_called_once_with(
        ["transactions", settings.KAFKA_OWNERSHIP_GRAPH_TOPIC]
    )

def test_process_message(mock_enricher):
    """Test _process_message method with valid data."""
    consumer_app = KafkaConsumer(
        consumer_group_id="test_group",
        input_topic="transactions",
        output_topic="enriched_transactions",
        bootstrap_servers="localhost:9092"
    )
    sample_tx = {"Time": 1.0, "Amount": 100.0, "V1": 0.5, "transaction_id": "tx_abc"}
    
    processed_data = consumer_app._process_message(json.dumps(sample_tx).encode('utf-8'))
    
    mock_enricher.return_value.enrich_transaction.assert_called_once_with(sample_tx)
    assert 'kyc_info' in processed_data
    assert 'ownership_links' in processed_data
    assert processed_data['transaction_id'] == "tx_abc"


def test_process_message_missing_transaction_id_generation(mock_enricher):
    """Test _process_message generates transaction_id if missing."""
    consumer_app = KafkaConsumer(
        consumer_group_id="test_group",
        input_topic="transactions",
        output_topic="enriched_transactions",
        bootstrap_servers="localhost:9092"
    )
    sample_tx_no_id = {"Time": 1.0, "Amount": 100.0, "V1": 0.5, "V2": 0.6} 
    
    processed_data = consumer_app._process_message(json.dumps(sample_tx_no_id).encode('utf-8'))
    
    assert 'transaction_id' in processed_data
    assert len(processed_data['transaction_id']) == 64

def test_handle_ownership_graph_message(mock_enricher):
    """Test _handle_ownership_graph_message reloads ownership data."""
    consumer_app = KafkaConsumer(
        consumer_group_id="test_group",
        input_topic="transactions",
        output_topic="enriched_transactions",
        bootstrap_servers="localhost:9092"
    )
    mock_msg_value = json.dumps({"company": "new_company"}).encode('utf-8')
    consumer_app._handle_ownership_graph_message(mock_msg_value)
    mock_enricher.return_value._load_ownership_graph_data.assert_called_once()


def test_process_and_save_batch(mock_save_dataframe_to_db):
    """Test _process_and_save_batch method."""
    consumer_app = KafkaConsumer(
        consumer_group_id="test_group",
        input_topic="transactions",
        output_topic="enriched_transactions",
        bootstrap_servers="localhost:9092"
    )
    test_batch = [
        {"transaction_id": "tx1", "Time": 1.0, "Amount": 10.0, "kyc_info": {"name": "UserA"}, "ownership_links": []},
        {"transaction_id": "tx2", "Time": 2.0, "Amount": 20.0, "kyc_info": {"name": "UserB"}, "ownership_links": [{"co": "XYZ"}]}
    ]
    
    consumer_app._process_and_save_batch(test_batch)
    
    mock_save_dataframe_to_db.assert_called_once()
    args, kwargs = mock_save_dataframe_to_db.call_args
    saved_df = args[0]
    assert kwargs['table_name'] == 'enriched_transactions'
    assert len(saved_df) == 2
    assert 'transaction_id' in saved_df.columns
    assert 'kyc_name' in saved_df.columns
    assert 'ownership_links_json' in saved_df.columns
    assert saved_df['ownership_links_json'].iloc[1] == json.dumps([{"co": "XYZ"}])


def test_start_consuming_transaction_message(mock_kafka_consumer, mock_kafka_producer, mock_enricher, mock_save_dataframe_to_db):
    """Test start_consuming with a transaction message."""
    consumer_app = KafkaConsumer(
        consumer_group_id="test_group",
        input_topic="transactions",
        output_topic="enriched_transactions",
        bootstrap_servers="localhost:9092"
    )
    consumer_app.running = True

    mock_tx_msg = create_mock_message("transactions", "tx_key", {"Time": 1.0, "Amount": 50.0, "transaction_id": "tx_abc"})
    
    mock_kafka_consumer.poll.side_effect = [mock_tx_msg, None, None] 
    
    consumer_app.batch_size = 1
    consumer_app.start_consuming()

    mock_enricher.return_value.enrich_transaction.assert_called_once()
    mock_kafka_producer.produce.assert_called_once()
    mock_save_dataframe_to_db.assert_called_once()
    mock_kafka_consumer.close.assert_called_once()

def test_start_consuming_ownership_message(mock_kafka_consumer, mock_kafka_producer, mock_enricher, mock_save_dataframe_to_db):
    """Test start_consuming with an ownership graph update message."""
    consumer_app = KafkaConsumer(
        consumer_group_id="test_group",
        input_topic="transactions",
        output_topic="enriched_transactions",
        bootstrap_servers="localhost:9092"
    )
    consumer_app.running = True

    mock_ownership_msg = create_mock_message(settings.KAFKA_OWNERSHIP_GRAPH_TOPIC, "comp_key", {"company_number": "NEWCO", "rel": "owner"})
    
    mock_kafka_consumer.poll.side_effect = [mock_ownership_msg, None, None]
    
    consumer_app.batch_size = 1
    consumer_app.start_consuming()

    mock_enricher.return_value._load_ownership_graph_data.assert_called_once()
    mock_kafka_producer.produce.assert_not_called()
    mock_save_dataframe_to_db.assert_not_called()
    mock_kafka_consumer.close.assert_called_once()

def test_start_consuming_error_message(mock_kafka_consumer):
    """Test start_consuming handles a Kafka error message."""
    consumer_app = KafkaConsumer(
        consumer_group_id="test_group",
        input_topic="transactions",
        output_topic="enriched_transactions",
        bootstrap_servers="localhost:9092"
    )
    consumer_app.running = True
    
    mock_error_msg = create_mock_message("transactions", None, {}, error=KafkaException("Mock Error"))
    
    mock_kafka_consumer.poll.side_effect = [mock_error_msg, None]
    
    with patch('app.consumer.logger.error') as mock_log_error:
        consumer_app.start_consuming()
        mock_log_error.assert_called()
    
    mock_kafka_consumer.close.assert_called_once()
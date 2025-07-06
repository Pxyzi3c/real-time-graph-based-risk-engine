import json
import os
import logging

from datetime import datetime
from kafka import KafkaConsumer
from app.enricher import enrich_transaction
from config.settings import settings
from config.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

OUTPUT_DIR = settings.STREAMING_OUTPUT_DIR
os.makedirs(OUTPUT_DIR, exist_ok=True)

consumer = KafkaConsumer(
    settings.TRANSACTIONS_TOPIC,
    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    enable_auto_commit=True
)

for message in consumer:
    tx = message.value
    enriched_tx = enrich_transaction(tx)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    with open(f"OUTPUT_DIR/tx_{ts}.json", "w") as f:
        json.dump(enriched_tx, f, indent=4)
        
    logger.info(f"Enriched transaction saved to file: OUTPUT_DIR/tx_{ts}.json")
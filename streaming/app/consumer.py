from kafka import KafkaConsumer
import json
from app.enricher import enrich_transaction
from config.settings import settings

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
    print(enriched_tx)
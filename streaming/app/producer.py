import json
import pandas as pd
from kafka import KafkaProducer
from dotenv import load_dotenv
from config.settings import settings

load_dotenv()
KAFKA_SERVER = settings.KAFKA_BOOTSTRAP_SERVERS
TOPIC = settings.TRANSACTION_TOPIC

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

df = pd.read_csv(settings.KAGGLE_OUTPUT_PATH)
print(f"Producing {len(df)} records to topic '{TOPIC}'...")

for _, row in df.iterrows():
    producer.send(TOPIC, row.to_dict())

producer.flush()
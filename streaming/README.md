# Streaming:
### _Kafka_
* Kafka streams → enrich transactions with KYC and ownership links.

## Goal:
Simulate Kafka-based transaction streams; enrich with ownership + KYC.

## Objectives:
* Set up Kafka + Zookeeper containers in `docker-compose`.
* Produce fake transactions from Kaggle dataset into `transactions` topic.
* Add enrichment stage: join KYC + OpenCorporates synthetic graph.
* Build Python Kafka consumer-producer app.
* (Optional): Use Faust or Confluent Python SDK for streaming abstraction.

## Kafka setup

1. Run docker to activate Kafka and Zookeeper
```bash
docker compose up -d
```

2. Connect to Kafka Container and Create Topics
> [!NOTE]
> Already accomplished when you run the docker compose through init_kafka
```bash
docker exec kafka kafka-topics --create --topic transactions --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1
docker exec kafka kafka-topics --create --topic ownership_graph --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1
docker exec kafka kafka-topics --create --topic enriched_transactions --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1
```

3. Verify Topic Creation
```bash
docker exec kafka kafka-topics --list --bootstrap-server kafka:9092
```
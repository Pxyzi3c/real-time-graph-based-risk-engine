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
Access kafka command line
```bash
docker exec -it kafka bash
```
Create kafka topics
```bash
kafka-topics --create --topic transactions --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1
kafka-topics --create --topic ownership_graph --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1
kafka-topics --create --topic enriched_transactions --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1
```

3. Verify Topic Creation
```bash
kafka-topics --list --bootstrap-server kafka:9092
```

4. Execute Producers
Once all containers are up, you can execute the producer script within the streaming container.
```bash
docker exec -it streaming python app/producer.py
```

5. Verify messages in topic
```bash
# To consume from transactions topic (run in a new terminal)
kafka-console-consumer --bootstrap-server localhost:9092 --topic transactions --from-beginning

# To consume from ownership_graph topic (run in another new terminal)
kafka-console-consumer --bootstrap-server localhost:9092 --topic ownership_graph --from-beginning
```
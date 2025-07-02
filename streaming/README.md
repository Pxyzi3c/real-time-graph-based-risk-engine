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
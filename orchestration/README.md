# Orchestration Layer:
### _Apache Airflow_
    * Daily refresh of entity graph
    * Nightly fraud ring scan
    * Trigger alert pipeline

## Goal:
Automate and schedule all pipelines via Airflow.

## Objectives:
* Add Airflow service container.
* Setup DAGs:
    * Daily OpenCorporates fetcher
    * Nightly fraud graph scan
    * Daily parquet dump
* Write custom PythonOperators for task logic.
* Mount shared volume between ingestion/airflow for pipeline handoff.
* Enable XCom or DB-based state tracking if needed.
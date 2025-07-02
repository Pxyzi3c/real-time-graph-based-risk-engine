# Ingestion Layer:
### _Extraction_
  * Python script → sends to Kafka topic "transactions"
  * Scheduled pull from OpenCorporates → "ownership_graph"

## Goal:
Pull data from Kaggle and OpenCorporates API (or synthetic), preprocess, and push to PostgreSQL.

## Objectives:
* Design ingestion module as a Docker container.
* Set up virtual env + requirements.
* Implement `extract-clean-transform-load` pipeline.
* Save cleaned Kaggle data to PostgreSQL.
* Add synthetic OpenCorporates relationship data (fallback if API fails).
* Create ingestion CLI (e.g. `python main.py --source kaggle`).
* Mount `./data/` for CSV I/O consistency.
* Unit test transformations (dropna, scaling, etc).
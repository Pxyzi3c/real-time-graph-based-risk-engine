# Storage:
### _PostgreSQL, Parquet, Neo4j_
* PostgreSQL for structured user/account info
* Parquet on GCP Storage for historical transaction logs
* Neo4j (free community version) to store and analyze entity relationships

## Goal:
Store entity metadata, historical logs, and relationship graph.

## Objectives:
* PostgreSQL for tabular data (transactions, ownership, KYC).
* Design and initialize SQL schema.
* Mount persistent volume `pg_data` for durability.
* Neo4j Docker setup (for graph relationships).
* Test data load into both PostgreSQL and Neo4j from ingestion output.
* Save raw events to Parquet (GCP-compatible local folder).
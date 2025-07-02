# Transformation Layer:
### _DBT_
    * Flag rapid intra-network transfers
    * Detect time-bound circular fund movements

## Goal:
Analytical modeling on enriched data.

## Objectives:
* Connect dbt to PostgreSQL (shared schema).
* Write dbt models:
    * `flag_rapid_transfers.sql`
    * `detect_circular_funds.sql`
    * `high_risk_clusters.sql`
* Add model tests + freshness policies.
* Use dbt snapshots if ownership changes.
* Automate dbt runs via Airflow DAG.
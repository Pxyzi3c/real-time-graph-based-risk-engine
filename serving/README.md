# Serving:
### _FastAPI_
* /alerts → returns list of accounts in high-risk graph clusters
* /graph?id=1234 → visualize user’s connected entities

## Goal:
Expose endpoints for alerts, graph visualization, status.

## Objectives:
* Create FastAPI service container.
* Implement endpoints:
    * `GET /alerts`
    * `GET /graph?id=123`
    * `GET /health`
* Connect to PostgreSQL and Neo4j.
* Add data validation with Pydantic models.
* Container health checks + logging.
# Real-Time Graph-Based Risk Engine for Money Laundering Rings

## Set Up Python Virtual Environment

1. Create Virtual Environment:
    * Install ```virtualenv``` (if not installed already):
    ```bash
    pip install virtualenv
    ```
    * Create and activate a virtual environment:
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: .\venv\Scripts\activate
    ```
2. Upgrade pip:
```bash
pip install --upgrade pip
```

## Install Core Project Dependencies

1. Install Dependencies:
    * Install the necessary dependencies for Kafka, PostgreSQL, Neo4j, dbt, Airflow, FastAPI, and Superset:
    ```bash
    pip install kafka-python psycopg2-binary fastapi neo4j dbt-core dbt-postgres apache-airflow apache-superset
    ```
2. Generate ```requirements.txt```:
    * Generate a ```requirements.txt``` file for easy dependency management:
    ```bash
    pip freeze > requirements.txt
    ```
3. Commit Dependencies:
    * Commit the changes:
    ```bash
    git add .
    git commit -m "Set up initial project structure and installed dependencies"
    git push origin main
    ```
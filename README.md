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

## Set Up Project Structure

1. Directory Structure:
    * Create the following directories to organize the project:
    ```bash
    mkdir src data logs
    mkdir src/ingestion src/streaming src/storage src/orchestration src/transformation src/serving src/visualization
    ```
2. Ensure Organized Folder Layout:
    * Your project should now look like this:
    ```bash
    real-time-aml-risk-engine/
    ├── data/                # Folder to store data files
    │   ├── raw/             # Raw data (e.g., Credit Card Fraud Dataset)
    │   └── processed/       # Processed data (e.g., enriched transaction data)
    ├── logs/                # Folder for logs (e.g., Airflow logs)
    ├── src/                 # All source code for the project
    │   ├── ingestion/       # Scripts for ingesting data into Kafka
    │   ├── streaming/       # Kafka consumer and streaming logic
    │   ├── storage/         # PostgreSQL and Neo4j storage logic
    │   ├── orchestration/   # Airflow DAGs and scheduling
    │   ├── transformation/  # dbt transformation logic
    │   ├── serving/         # FastAPI for exposing endpoints
    │   └── visualization/   # Superset configurations and dashboard logic
    ├── requirements.txt     # List of project dependencies
    ├── README.md            # Project description and setup instructions
    └── .gitignore           # To ignore unnecessary files in Git
    ```
3. Create the ```README.md``` file:
    * Add a brief description of the project and a placeholder for detailed setup instructions later.
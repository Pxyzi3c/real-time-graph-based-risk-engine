from app.db import save_dataframe_to_db
from app.neo4j_handler import Neo4jHandler
from app.parquet_saver import save_to_parquet
import pandas as pd

# Simulated transaction
sample = pd.DataFrame([{"transaction_id": "tx001", "amount": 1200, "customer_id": "cust001"}])

# PostgreSQL
save_dataframe_to_db(sample, "test_table")

# Parquet
print("Saved to:", save_to_parquet(sample))

# Neo4j
neo = Neo4jHandler()
neo.create_user_node("cust001", "John Doe")
neo.close()
from neo4j import GraphDatabase
from config.settings import settings

import os

class GraphLoader:
    def __init__(self):
        self.driver = GraphDatabase.driver(settings.NEO4J_URI, auth=(settings.NEO4J_USER, settings.NEO4J_PASS))

    def close(self):
        self.driver.close()

    def insert_transaction(self, tx_data):
        query = """
        MERGE (c:Customer {id: $customer_id})
        MERGE (t:Transaction {id: $transaction_id, amount: $amount, risk_score: $risk_score})
        MERGE (c)-[:MADE]->(t)
        """
        with self.driver.session() as session:
            session.run(query, tx_data)
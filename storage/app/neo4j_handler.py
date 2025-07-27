import os

from neo4j import GraphDatabase
from dotenv import load_dotenv
from config.settings import settings

load_dotenv()

class Neo4jHandler:
    def __init__(self):
        uri = settings.NEO4J_URI
        self.driver = GraphDatabase.driver(uri, auth=(settings.NEO4J_USER, settings.NEO4J_PASS))

    def close(self):
        self.driver.close()

    def create_user_node(self, user_id, name):
        query = "MERGE (u:User {id: $user_id, name: $name})"
        with self.driver.session() as session:
            session.run(query, user_id=user_id, name=name)
import logging
import pandas as pd
from typing import Dict, List
from streaming.app.utils import get_db_connection
from streaming.config.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def load_ownership_graph_from_postgres() -> Dict[str, List[str]]:
    conn = None
    try:
        conn = get_db_connection()
        query = "SELECT owner_id, owned_id FROM company_ownership_links;"
        df = pd.read_sql(query, conn)

        ownership_graph = {}
        for _, row in df.iterrows():
            owner = str(row['owner_id'])
            owned = str(row['owned_id'])
            if owner not in ownership_graph:
                ownership_graph[owner] = []
            ownership_graph[owner].append(owned)
        logger.info(f"Loaded {len(df)} ownership links from PostgreSQL.")
        return ownership_graph
    except Exception as e:
        logger.error(f"Error loading ownership graph from PostgreSQL: {e}")
        return {}
    finally:
        if conn:
            conn.close()

def get_owned_entities(entity_id: str, ownership_graph: Dict[str, List[str]]) -> List[str]:
    return ownership_graph.get(entity_id, [])
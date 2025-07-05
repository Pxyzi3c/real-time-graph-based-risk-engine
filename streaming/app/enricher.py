from app.kyc_loader import get_kyc_metadata
from app.ownership_graph_loader import get_ownership_links

def enrich_transaction(tx: dict) -> dict:
    user_id = tx.get("UserID", "unknown")
    tx["kyc"] = get_kyc_metadata(user_id)
    tx["ownership_links"] = get_ownership_links(user_id)
    return tx
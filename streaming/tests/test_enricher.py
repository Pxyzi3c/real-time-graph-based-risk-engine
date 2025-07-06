from app.enricher import enrich_transaction

def test_enrich_transaction_keys():
    sample = {"UserID": "123"}
    enriched = enrich_transaction(sample)
    
    assert "kyc" in enriched
    assert "ownership_links" in enriched
    assert isinstance(enriched["kyc"], dict)
    assert isinstance(enriched["ownership_links"], list)
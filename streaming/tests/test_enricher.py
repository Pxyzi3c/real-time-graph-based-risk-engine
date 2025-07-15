import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import pandas as pd
import app.db as db
from unittest.mock import patch, MagicMock
from app.enricher import TransactionEnricher

def test_enricher_initialization():
    """Test if the enricher loads data correctly upon initialization."""
    enricher = TransactionEnricher()
    assert not enricher.kyc_data.empty, "KYC data should not be empty after initialization."
    assert not enricher.ownership_graph.empty, "Ownership graph data should not be empty after initialization."
    assert len(enricher.kyc_data) == 5, "KYC data should contain 5 records."
    assert len(enricher.ownership_graph) == 3, "Ownership graph should contain 3 records."

def test_enrich_transaction_no_kyc_data():
    """Test enrichment when no KYC data is available."""
    with patch('app.db.get_dataframe_from_db', return_value=pd.DataFrame()):
        enricher = TransactionEnricher()
        assert enricher.kyc_data.empty
        
        sample_transaction = {
            "Time": 1.0,
            "Amount": 100.0,
            "Class": 0
        }
        enriched_tx = enricher.enrich_transaction(sample_transaction)
        assert 'customer_id' not in enriched_tx # No customer_id assigned without KYC
        assert 'kyc_info' in enriched_tx
        assert enriched_tx['kyc_info'] == {} # Should be an empty dict
        assert 'ownership_links' in enriched_tx
        assert enriched_tx['ownership_links'] == []
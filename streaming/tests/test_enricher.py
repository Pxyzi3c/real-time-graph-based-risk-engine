import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from app.enricher import TransactionEnricher

def test_enricher_initialization():
    """Test if the enricher loads data correctly upon initialization."""
    enricher = TransactionEnricher()
    print("KYC Data:", enricher)
    assert not enricher.kyc_data.empty, "KYC data should not be empty after initialization."
    assert not enricher.ownership_graph.empty, "Ownership graph data should not be empty after initialization."
    assert len(enricher.kyc_data) == 5, "KYC data should contain 5 records."
    assert len(enricher.ownership_graph) == 3, "Ownership graph should contain 3 records."

def test_enrich_transaction_with_kyc_and_ownership():
    """Test the enrichment of a single transaction."""
    enricher = TransactionEnricher()
    
    sample_transaction = {
        "Time": 1.0,
        "V1": -1.359807,
        "V2": -0.072781,
        "V3": 2.536347,
        "Amount": 149.62,
        "Class": 0
        # In a real scenario, this would have a customer_id
    }

    # Temporarily modify the enricher's internal method to control which KYC is picked
    with patch.object(enricher.kyc_data, 'sample') as mock_sample:
        mock_sample.return_value = MagicMock(index=['CUST00001']) # Force a specific customer_id
        
        enriched_tx = enricher.enrich_transaction(sample_transaction)

        assert 'customer_id' in enriched_tx
        assert enriched_tx['customer_id'] == 'CUST00001' # Assert the forced customer_id
        assert 'kyc_info' in enriched_tx
        assert 'full_name' in enriched_tx['kyc_info']
        assert 'country' in enriched_tx['kyc_info']
        assert 'ownership_links' in enriched_tx # Should be present even if empty for now

        # Verify that KYC data matches the mocked data for CUST00001
        expected_kyc = enricher.kyc_data.loc['CUST00001'].to_dict()
        for key, value in expected_kyc.items():
            assert enriched_tx['kyc_info'][key] == value

        # As per current design, ownership_links are empty.
        assert enriched_tx['ownership_links'] == []

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
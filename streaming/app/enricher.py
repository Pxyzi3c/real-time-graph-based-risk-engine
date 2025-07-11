import logging
import pandas as pd
from app.db import get_dataframe_from_db
from config.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

class TransactionEnricher:
    def __init__(self):
        self.kyc_data = self._load_kyc_data()
        self.ownership_graph = self._load_ownership_graph_data()
        logger.info("TransactionEnricher initialized. KYC and Ownership data loaded.")

    def _load_kyc_data(self) -> pd.DataFrame:
        """Loads KYC data from PostgreSQL."""
        try:
            kyc_df = get_dataframe_from_db('customer_kyc')
            kyc_df.set_index('customer_id', inplace=True)
            logger.info(f"Loaded {len(kyc_df)} KYC records.")
            return kyc_df
        except Exception as e:
            logger.error(f"Failed to load KYC data: {e}")
            return pd.DataFrame() # Return empty DataFrame on failure

    def _load_ownership_graph_data(self) -> pd.DataFrame:
        """Loads ownership graph data from PostgreSQL."""
        try:
            ownership_df = get_dataframe_from_db('company_ownership_links')
            logger.info(f"Loaded {len(ownership_df)} ownership graph links.")
            return ownership_df
        except Exception as e:
            logger.error(f"Failed to load ownership graph data: {e}")
            return pd.DataFrame() # Return empty DataFrame on failure

    def enrich_transaction(self, transaction: dict) -> dict:
        """
        Enriches a single transaction with KYC and ownership information.
        
        Args:
            transaction (dict): A dictionary representing a single transaction.
            
        Returns:
            dict: The enriched transaction dictionary.
        """
        enriched_transaction = transaction.copy()

        # Simulate customer_id from transaction (e.g., if 'Card' maps to customer ID)
        # For the Kaggle dataset, 'Card' doesn't directly map to a customer_id.
        # We need a way to link transactions to customers for KYC enrichment.
        # For this simulation, let's assume 'V1' from the Kaggle dataset can be
        # deterministically mapped to a 'customer_id' for joining purposes.
        # This is a simplification for the project's scope, in a real scenario,
        # transaction data would contain 'customer_id' directly.
        # Let's map it based on an arbitrary rule for demonstration.
        # For now, let's just assign a random KYC for simplicity in this demo.
        
        # NOTE: This is a placeholder for a proper customer_id mapping.
        # In a real system, 'customer_id' would be part of the incoming transaction stream.
        # For now, we'll assign a random existing customer_id from our fake KYC data.
        if not self.kyc_data.empty:
            random_customer_id = self.kyc_data.sample(1).index[0]
            enriched_transaction['customer_id'] = random_customer_id
            
            # Enrich with KYC data
            kyc_info = self.kyc_data.loc[random_customer_id].to_dict()
            enriched_transaction['kyc_info'] = kyc_info
        else:
            enriched_transaction['kyc_info'] = {}
            logger.warning("No KYC data available for enrichment.")

        # Enrich with ownership graph data
        # This part requires linking the transaction to a company, which then links to the ownership graph.
        # For the Kaggle dataset, this is highly abstract.
        # In a real scenario, transactions might involve business accounts,
        # which can be linked to companies in the ownership graph.
        # For demonstration, we'll assume a dummy link for now or skip until a clear mapping exists.
        
        # If we had a 'merchant_company_number' in transaction, we could do:
        # merchant_company_number = transaction.get('merchant_company_number')
        # if merchant_company_number and not self.ownership_graph.empty:
        #     related_ownership = self.ownership_graph[self.ownership_graph['company_number'] == merchant_company_number]
        #     enriched_transaction['ownership_links'] = related_ownership.to_dict(orient='records')
        # else:
        #     enriched_transaction['ownership_links'] = []
        
        # For now, let's just add a placeholder for ownership links.
        # In a real system, this would involve sophisticated graph traversal.
        enriched_transaction['ownership_links'] = []
        
        logger.debug(f"Enriched transaction: {enriched_transaction.get('Time')}")
        return enriched_transaction
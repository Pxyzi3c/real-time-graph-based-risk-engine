import logging
import json
import pandas as pd
from typing import Dict, Any
from config.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

class TransactionEnricher:
    def __init__(self):
        self.ownership_graph_data: Dict[str, Any] = {}
        self.kyc_data: Dict[str, Any] = {}
        logger.info("TransactionEnricher initialized.")

    def load_ownership_graph(self, data: Dict[str, Any]):
        company_number = data.get("company_number")
        if company_number:
            self.ownership_graph_data[company_number] = data
            logger.debug(f"Loaded ownership data for company: {company_number}")

    def load_kyc_data(self, data: Dict[str, Any]):
        account_id = data.get("account_id")
        if account_id:
            self.kyc_data[account_id] = data
            logger.debug(f"Loaded KYC data for account: {account_id}")

    def enrich_transaction(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        enriched_transaction = transaction.copy()

        card_id = enriched_transaction.get('V1_card_id')
        if card_id and str(card_id) in self.ownership_graph_data:
            enriched_transaction['ownership_info'] = self.ownership_graph_data[str(card_id)]
            logger.info(f"Enriched transaction {transaction.get('id')} with ownership info for card_id {card_id}.")
        else:
            enriched_transaction['ownership_info'] = None
            logger.debug(f"No ownership info found for card_id: {card_id}")


        dummy_account_id = enriched_transaction.get('V1_card_id')
        if dummy_account_id:
            if str(dummy_account_id) in self.kyc_data:
                enriched_transaction['kyc_info'] = self.kyc_data[str(dummy_account_id)]
                logger.info(f"Enriched transaction {transaction.get('id')} with KYC info for account {dummy_account_id}.")
            else:
                synthetic_kyc = {
                    "account_id": dummy_account_id,
                    "customer_name": f"Customer {dummy_account_id}",
                    "country": "US",
                    "risk_score": 0.5 + (hash(str(dummy_account_id)) % 100) / 200.0 # Dummy risk score
                }
                enriched_transaction['kyc_info'] = synthetic_kyc
                logger.debug(f"Generated synthetic KYC info for account: {dummy_account_id}")
        else:
            enriched_transaction['kyc_info'] = None
            logger.warning(f"No valid account identifier found in transaction for KYC enrichment.")

        return enriched_transaction
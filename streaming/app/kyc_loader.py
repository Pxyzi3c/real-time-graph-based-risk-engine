import logging
import random
from typing import Dict

logger = logging.getLogger(__name__)

KYC_DATA = {
    "account_1": {
        "country": random.choice(["PH", "US", "CN", "DE", "NG"]), 
        "device_id": f"device_{random.randint(1000, 9999)}",
        "email_domain": random.choice(["gmail.com", "outlook.com", "yahoo.com"])
    },
    "account_2": {
        "country": random.choice(["PH", "US", "CN", "DE", "NG"]),
        "device_id": f"device_{random.randint(1000, 9999)}",
        "email_domain": random.choice(["gmail.com", "outlook.com", "yahoo.com"])
    },
    "account_3": {
        "country": random.choice(["PH", "US", "CN", "DE", "NG"]),
        "device_id": f"device_{random.randint(1000, 9999)}",
        "email_domain": random.choice(["gmail.com", "outlook.com", "yahoo.com"])
    },
}

def load_kyc_data() -> Dict[str, Dict]:
    logger.info("Simulating KYC data loading...")
    return KYC_DATA

def get_kyc_for_account(account_id: str) -> Dict:
    kyc_data = load_kyc_data()
    return kyc_data.get(account_id, {})
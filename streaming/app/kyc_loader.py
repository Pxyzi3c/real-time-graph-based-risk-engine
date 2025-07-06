import random

def get_kyc_metadata(user_id: str) -> dict:
    return {
        "country": random.choice(["PH", "US", "CN", "DE", "NG"]),
        "device_id": f"device_{random.randint(1000, 9999)}",
        "email_domain": random.choice(["gmail.com", "outlook.com", "yahoo.com"]),
    }
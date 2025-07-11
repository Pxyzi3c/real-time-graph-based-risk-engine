import logging
import pandas as pd
import random
from faker import Faker
from datetime import datetime
from app.db import save_dataframe_to_db, get_dataframe_from_db
from config.settings import settings
from config.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)
fake = Faker()

def generate_fake_kyc_data(num_records: int) -> pd.DataFrame:
    """
    Generates a DataFrame of fake KYC data.
    """
    logger.info(f"Generating {num_records} fake KYC records...")
    data = []
    for i in range(num_records):
        data.append({
            "customer_id": f"CUST{i:05d}",
            "full_name": fake.name(),
            "email": fake.email(),
            "phone_number": fake.phone_number(),
            "address": fake.address(),
            "country": random.choice(['us_ca', 'gb', 'au', 'de', 'fr']),
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
            "gender": random.choice(['Male', 'Female', 'Other']),
            "occupation": fake.job(),
            "registration_date": fake.date_time_between(start_date="-5y", end_date="now").isoformat(),
            "last_login_ip": fake.ipv4_public(),
            "device_id": fake.uuid4(),
            "kyc_status": random.choice(['verified', 'pending', 'rejected']),
            "risk_score": round(random.uniform(0.1, 0.99), 2),
            "updated_at": datetime.now().isoformat()
        })
    df = pd.DataFrame(data)
    logger.info(f"Generated {num_records} fake KYC records.")
    return df

def save_kyc_data_to_db(df: pd.DataFrame):
    """
    Saves the KYC DataFrame to the PostgreSQL database.
    """
    try:
        save_dataframe_to_db(df, "customer_kyc", if_exists="replace", chunksize=settings.DB_SAVE_CHUNKSIZE)
        logger.info("Fake KYC data successfully written to PostgreSQL table 'customer_kyc'.")
    except Exception as e:
        logger.error(f"Failed to save fake KYC data to PostgreSQL: {e}")
        raise

def generate_and_save_kyc_data(num_records: int):
    """
    Generates fake KYC data and saves it to the database.
    """
    df = generate_fake_kyc_data(num_records)
    save_kyc_data_to_db(df)
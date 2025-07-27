CREATE TABLE IF NOT EXISTS customer_kyc (
    customer_id VARCHAR(255) PRIMARY KEY,
    full_name VARCHAR(255),
    email VARCHAR(255),
    phone_number VARCHAR(50),
    address TEXT,
    country VARCHAR(100),
    date_of_birth DATE,
    gender VARCHAR(50),
    occupation VARCHAR(255),
    registration_date TIMESTAMP,
    last_login_ip VARCHAR(50),
    device_id VARCHAR(255),
    kyc_status VARCHAR(50),
    risk_score NUMERIC(5,2),
    updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS company_ownership_links (
    company_number VARCHAR(255) PRIMARY KEY,
    jurisdiction_code VARCHAR(255),
    company_name VARCHAR(255),
    source_url VARCHAR(255),
    source_description VARCHAR(255),
    relationship_type VARCHAR(255),
    related_entity_name VARCHAR(255),
    related_entity_type VARCHAR(255),
    related_entity_role VARCHAR(255),
    retrieved_at timestamp without time zone
);

-- Add indexes for common lookup fields if necessary for performance on large datasets
CREATE INDEX IF NOT EXISTS idx_customer_kyc_country ON customer_kyc (country);
CREATE INDEX IF NOT EXISTS idx_company_ownership_country ON company_ownership_links (country);
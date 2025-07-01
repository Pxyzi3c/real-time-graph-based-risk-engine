CREATE TABLE IF NOT EXISTS company_ownership_links (
    company_number VARCHAR,
    jurisdiction_code VARCHAR,
    company_name VARCHAR,
    source_url TEXT,
    source_description TEXT,
    relationship_type VARCHAR,
    related_entity_name VARCHAR,
    related_entity_type VARCHAR,
    related_entity_role VARCHAR,
    retrieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
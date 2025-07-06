import random

def get_ownership_links(user_id: str):
    companies = ["Acme Corp", "ShellWorks Ltd", "Nominee Inc", "FrontCo"]
    relationships = ["owner", "director", "shareholder"]

    return [
        {
            "related_entity_name": random.choice(companies),
            "related_entity_role": random.choice(relationships),
            "confidence": round(random.uniform(0.6, 0.99), 2)
        }
        for _ in range(random.randint(1, 3))       
    ]
import os
from dotenv import load_dotenv

print("Loading config.py")

load_dotenv()

DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "database": os.getenv("DB_NAME"),
}

print(f"DB_CONFIG: {DB_CONFIG}")

PERFORMANCE_SCHEMA_CONFIG = {
    **DB_CONFIG,
    "database": "performance_schema",
}

COLLECTION_DURATION = int(os.getenv("COLLECTION_DURATION", 30))
COLLECTION_INTERVAL = float(os.getenv("COLLECTION_INTERVAL", 0.0001))

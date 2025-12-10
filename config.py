# config.py

import os

# ---------- PATHS ----------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SOURCES_DIRS = {
    "sap":      os.path.join(BASE_DIR, "sources", "sap"),
    "vault":    os.path.join(BASE_DIR, "sources", "vault"),
    "powerbi":  os.path.join(BASE_DIR, "sources", "powerbi"),
    "pos":      os.path.join(BASE_DIR, "sources", "pos"),
    "invoices": os.path.join(BASE_DIR, "sources", "invoices"),
}

USER_UPLOAD_DIR = os.path.join(BASE_DIR, "user_uploads")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------- DATABASE ----------
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "taxonomy_poc",
    "user": "postgres",
    "password": "postgres123",   # change if different
}

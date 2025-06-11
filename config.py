"""
General configuration for Netflix package
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "database": os.getenv("DB_DATABASE", "netflix_ma_db"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT", "5432"),
}

GEMINI_CONFIG = {
    "project_id": os.getenv("GCP_PROJECT_ID"),
    "location": os.getenv("GCP_LOCATION", "us-central1")
}
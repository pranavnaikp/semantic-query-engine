# config.py

"""
Configuration for the semantic analytics engine.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "analytics"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# OpenAI configuration (for intent extraction only)
OPENAI_CONFIG = {
    "api_key": os.getenv("OPENAI_API_KEY"),
    "model": os.getenv("OPENAI_MODEL", "gpt-3.5-turbo"),
    "temperature": float(os.getenv("OPENAI_TEMPERATURE", 0.1)),
}

# Application configuration
APP_CONFIG = {
    "debug": os.getenv("DEBUG", "false").lower() == "true",
    "max_query_execution_time": int(os.getenv("MAX_QUERY_TIME", 30)),
    "default_limit": int(os.getenv("DEFAULT_LIMIT", 1000)),
    "enable_mock_data": os.getenv("ENABLE_MOCK_DATA", "true").lower() == "true",
}

# Semantic catalog configuration
CATALOG_CONFIG = {
    "refresh_interval": int(os.getenv("CATALOG_REFRESH_INTERVAL", 300)),  # seconds
    "cache_enabled": os.getenv("CATALOG_CACHE_ENABLED", "true").lower() == "true",
}
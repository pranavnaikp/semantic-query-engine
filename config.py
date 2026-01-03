"""
SIMPLIFIED MVP: Minimal configuration.
Only PostgreSQL and OpenAI (free tier).
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


# OpenAI (for intent extraction only)
OPENAI_CONFIG = {
    "api_key": os.getenv("OPENAI_API_KEY", ""),  # Optional for MVP
    "model": "gpt-3.5-turbo",  # Free tier model
    "temperature": 0.1
}

# PostgreSQL Configuration
POSTGRES_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "semantic_db"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
    "min_connections": 1,
    "max_connections": 5
}

# Application Configuration
APP_CONFIG = {
    "debug": os.getenv("DEBUG", "true").lower() == "true",
    "name": "Semantic Analytics MVP",
    "version": "1.0.0",
    
    # Query settings
    "max_query_time": 30,  # seconds
    "default_limit": 1000,
    
    # Mock data settings
    "enable_mock_data": os.getenv("ENABLE_MOCK_DATA", "true").lower() == "true",
    
    # Cache settings
    "use_cache": True,
    "cache_ttl": 300,  # 5 minutes
    
    # Server settings
    "host": "0.0.0.0",
    "port": 8000,
    
    # Free tier limits
    "max_queries_per_minute": 60,
    "max_metrics_per_tenant": 100,
    "max_dimensions_per_tenant": 50
}

# Logging Configuration
LOG_CONFIG = {
    "level": "INFO" if not APP_CONFIG["debug"] else "DEBUG",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
}

# Feature flags (disable complex features for MVP)
FEATURE_FLAGS = {
    "enable_schema_support": False,  # No multi-schema
    "enable_multi_tenant": False,    # No multi-tenant
    "enable_advanced_joins": False,  # Simple joins only
    "enable_real_time": False,       # No real-time
    "enable_email_reports": False    # No email
}


def check_config():
    """Check configuration and provide helpful messages."""
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Check if we have OpenAI API key
    if not OPENAI_CONFIG["api_key"]:
        logger.warning("⚠️  OPENAI_API_KEY not set. LLM intent extraction will use fallback mode.")
        logger.info("💡 Get a free API key at: https://platform.openai.com/api-keys")
    
    # Check PostgreSQL connection
    if not APP_CONFIG["enable_mock_data"]:
        logger.info(f"📦 PostgreSQL: {POSTGRES_CONFIG['database']}@{POSTGRES_CONFIG['host']}")
    
    # Welcome message
    if APP_CONFIG["debug"]:
        logger.info("🚀 Starting Semantic Analytics MVP (Debug Mode)")
        logger.info("📊 Features: Natural language → SQL → Visualization")
        logger.info("💰 Cost: $0 (PostgreSQL + OpenAI free tier)")
    
    # Helpful tips
    tips = []
    if APP_CONFIG["enable_mock_data"]:
        tips.append("• Using mock data - no database required")
    if not OPENAI_CONFIG["api_key"]:
        tips.append("• Using rule-based intent extraction (no OpenAI)")
    
    if tips:
        logger.info("💡 Tips:")
        for tip in tips:
            logger.info(f"  {tip}")


# Run config check on import
check_config()
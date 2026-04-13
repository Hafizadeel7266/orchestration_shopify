"""All configuration settings in one file."""

import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger("shopify_pipeline.settings")

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIRECTORY = os.getenv("LOG_DIRECTORY", "./logs/dlt")

# ============================================
# API CONFIGURATION
# ============================================
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2026-01")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "60"))

# ============================================
# PII HASHING
# ============================================
PII_HASH_SECRET = os.environ.get("PII_HASH_SECRET", "")

# ============================================
# INCREMENTAL LOADING CONFIGURATION
# ============================================
FETCH_LAST_DAYS = int(os.getenv("FETCH_LAST_DAYS", "7"))
INCREMENTAL_LOOKBACK_DAYS = int(os.getenv("INCREMENTAL_LOOKBACK_DAYS", "1"))

# ============================================
# BACKFILL CONFIGURATION
# ============================================
DEFAULT_BACKFILL_DAYS = int(os.getenv("DEFAULT_BACKFILL_DAYS", "0"))
MAX_BACKFILL_DAYS = int(os.getenv("MAX_BACKFILL_DAYS", "90"))
BACKFILL_BATCH_DAYS = int(os.getenv("BACKFILL_BATCH_DAYS", "30"))

# ============================================
# CHUNK & PAGE CONFIGURATION
# ============================================
DEFAULT_CHUNK_DAYS = int(os.getenv("DEFAULT_CHUNK_DAYS", "7"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "100"))
SHOPIFY_ORDERS_SNAPSHOT_LOOKBACK_DAYS = int(
  os.getenv("SHOPIFY_ORDERS_SNAPSHOT_LOOKBACK_DAYS", "7")
)
SHOPIFY_ORDERS_HYDRATE_BATCH_SIZE = int(
  os.getenv("SHOPIFY_ORDERS_HYDRATE_BATCH_SIZE", "25")
)
SHOPIFY_BULK_POLL_INTERVAL_SECONDS = int(
  os.getenv("SHOPIFY_BULK_POLL_INTERVAL_SECONDS", "5")
)
SHOPIFY_ORDERS_BULK_ID_SNAPSHOT_ENABLED = os.getenv(
  "SHOPIFY_ORDERS_BULK_ID_SNAPSHOT_ENABLED", "true"
).lower() in {"1", "true", "yes", "on"}

# ============================================
# DESTINATION CONFIGURATION                    ← NEW
# ============================================
DESTINATION = os.getenv("DESTINATION", "duckdb")

# ============================================
# DATABASE PATHS
# ============================================
BASE_DATA_PATH = os.getenv("BASE_DATA_PATH", "data/duckdb")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", f"{BASE_DATA_PATH}/shopify.duckdb")

# ============================================
# SNOWFLAKE CONFIGURATION                      ← NEW
# ============================================
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "")

# ============================================
# SHOP CREDENTIALS (from .env)
# ============================================
SHOPIFY_SHOP_URL = os.getenv("SHOPIFY_SHOP_URL", "trophysmack.myshopify.com")
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN", "")

# ============================================
# PIPELINE SETTINGS
# ============================================
PIPELINE_NAME = os.getenv("PIPELINE_NAME", "shopify_dlt")
DATASET_NAME = os.getenv("DATASET_NAME", "shopify_data")

# ============================================
# VALIDATION
# ============================================
def validate_settings():
  """Validate critical settings."""
  if not SHOPIFY_ACCESS_TOKEN:
      logger.warning("SHOPIFY_ACCESS_TOKEN not set in environment")

  if PAGE_SIZE not in [50, 100, 250]:
      logger.warning(f"PAGE_SIZE={PAGE_SIZE} may not be optimal. Recommended: 50, 100, or 250")

  if MAX_BACKFILL_DAYS > 90:
      logger.warning(f"MAX_BACKFILL_DAYS={MAX_BACKFILL_DAYS} exceeds recommended 90 days")

# Run validation on import
validate_settings()
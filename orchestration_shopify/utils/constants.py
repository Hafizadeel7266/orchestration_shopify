"""Shared constants for Shopify orchestration."""

# ── Asset Group Names ──
SHOPIFY_GROUP = "shopify"
SHOPIFY_BACKFILL_GROUP = "shopify_backfill"

# ── Resource Keys ──
SHOPIFY_RESOURCE_KEY = "shopify_pipeline"

# ── Tags ──
TEAM_TAG = "data-engineering"
PIPELINE_TAG = "shopify"

# ── Freshness Policies (in minutes) ──
ORDERS_MAX_LAG_MINUTES = 60           # 1 hour
CUSTOMERS_MAX_LAG_MINUTES = 360       # 6 hours
PRODUCTS_MAX_LAG_MINUTES = 720        # 12 hours
COLLECTIONS_MAX_LAG_MINUTES = 720     # 12 hours
SHOP_MAX_LAG_MINUTES = 1440           # 24 hours

# ── Schedule Crons ──
ORDERS_CRON = "*/30 * * * *"          # Every 30 minutes
CUSTOMERS_CRON = "0 */6 * * *"        # Every 6 hours
CATALOG_CRON = "0 */12 * * *"         # Every 12 hours
SHOP_CRON = "0 3 * * *"              # Daily at 3 AM UTC
FULL_SYNC_CRON = "0 2 * * 0"         # Sunday 2 AM UTC
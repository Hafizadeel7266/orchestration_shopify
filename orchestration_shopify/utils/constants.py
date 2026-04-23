"""Shared constants for Shopify orchestration."""

# ── Asset Group Names (sub-groups) ──
SHOPIFY_GROUP = "shopify"
ORDERS_GROUP = "orders"
CUSTOMERS_GROUP = "customers"
PRODUCTS_GROUP = "products"
COLLECTIONS_GROUP = "collections"
SHOP_GROUP = "shop"

# ── Owner ──
ASSET_OWNER = "team:data-engineering"

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
ORDERS_CRON = "*/30 * * * *"
CUSTOMERS_CRON = "0 */6 * * *"
CATALOG_CRON = "0 */12 * * *"
SHOP_CRON = "0 3 * * *"
FULL_SYNC_CRON = "0 2 * * 0"
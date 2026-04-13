"""Dagster Definitions — single entry point for the Shopify orchestration layer.

This file wires together all assets, resources, schedules, sensors, and jobs.
"""

import os
from dotenv import load_dotenv

from dagster import Definitions

from .assets import (
shopify_shop_asset,
shopify_orders_asset,
shopify_customers_asset,
shopify_products_asset,
shopify_collections_asset,
shopify_collection_products_asset,
)
from .schedules import all_schedules
from .sensors import all_sensors
from .jobs import all_jobs
from .resources import ShopifyPipelineResource
from .utils.constants import SHOPIFY_RESOURCE_KEY

# Load .env for local development
load_dotenv()

# =============================================================================
# RESOURCE CONFIGURATION (from environment variables)
# =============================================================================

shopify_resource = ShopifyPipelineResource(
shop_url=os.getenv("SHOPIFY_SHOP_URL", ""),
access_token=os.getenv("SHOPIFY_ACCESS_TOKEN", ""),
destination=os.getenv("DESTINATION", "duckdb"),
destination_path=os.getenv("DUCKDB_PATH", "data/duckdb/shopify.duckdb"),
pipeline_name=os.getenv("PIPELINE_NAME", "shopify_dlt"),
dataset_name=os.getenv("DATASET_NAME", "shopify_data"),
initial_load_past_days=int(os.getenv("FETCH_LAST_DAYS", "7")),
lookback_days=int(os.getenv("INCREMENTAL_LOOKBACK_DAYS", "1")),
chunk_days=int(os.getenv("DEFAULT_CHUNK_DAYS", "7")),
page_size=int(os.getenv("PAGE_SIZE", "100")),
max_rows=int(os.getenv("MAX_ROWS", "0")),
)

# =============================================================================
# ALL ASSETS
# =============================================================================

all_assets = [
shopify_shop_asset,
shopify_orders_asset,
shopify_customers_asset,
shopify_products_asset,
shopify_collections_asset,
shopify_collection_products_asset,
]

# =============================================================================
# DEFINITIONS — the single Dagster entry point
# =============================================================================

defs = Definitions(
assets=all_assets,
resources={
    SHOPIFY_RESOURCE_KEY: shopify_resource,
},
schedules=all_schedules,
sensors=all_sensors,
jobs=all_jobs,
)
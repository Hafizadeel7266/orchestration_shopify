"""Dagster job definitions for Shopify pipeline."""

from dagster import define_asset_job, AssetSelection

from ..utils.constants import SHOPIFY_GROUP, PIPELINE_TAG, TEAM_TAG

# ── Incremental Jobs (used by schedules) ──

orders_job = define_asset_job(
name="shopify_orders_job",
selection=AssetSelection.keys("shopify_orders"),
description="Incremental orders sync",
tags={"dagster/priority": "1", "pipeline": PIPELINE_TAG, "team": TEAM_TAG},
)

customers_job = define_asset_job(
name="shopify_customers_job",
selection=AssetSelection.keys("shopify_customers"),
description="Incremental customers sync",
tags={"pipeline": PIPELINE_TAG, "team": TEAM_TAG},
)

catalog_job = define_asset_job(
name="shopify_catalog_job",
selection=AssetSelection.keys(
    "shopify_products",
    "shopify_collections",
    "shopify_collection_products",
),
description="Sync products, collections, and collection-product bridge",
tags={"pipeline": PIPELINE_TAG, "team": TEAM_TAG},
)

shop_job = define_asset_job(
name="shopify_shop_job",
selection=AssetSelection.keys("shopify_shop"),
description="Daily shop metadata refresh",
tags={"pipeline": PIPELINE_TAG, "team": TEAM_TAG},
)

# ── Full Sync ──

full_sync_job = define_asset_job(
name="shopify_full_sync_job",
selection=AssetSelection.groups(SHOPIFY_GROUP),
description="All Shopify assets in one run",
tags={"pipeline": PIPELINE_TAG, "team": TEAM_TAG, "mode": "full_sync"},
)

all_jobs = [
orders_job,
customers_job,
catalog_job,
shop_job,
full_sync_job,
]
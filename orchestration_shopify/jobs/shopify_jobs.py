"""Dagster job definitions — one job per sub-group."""

from dagster import define_asset_job, AssetSelection

from ..utils.constants import (
    ORDERS_GROUP,
    CUSTOMERS_GROUP,
    PRODUCTS_GROUP,
    COLLECTIONS_GROUP,
    SHOP_GROUP,
    PIPELINE_TAG,
    TEAM_TAG,
)


orders_job = define_asset_job(
    name="shopify_orders_job",
    selection=AssetSelection.groups(ORDERS_GROUP),
    description="Sync all order tables",
    tags={
        "dagster/priority": "1",
        "pipeline": PIPELINE_TAG,
        "team": TEAM_TAG,
    },
)


customers_job = define_asset_job(
    name="shopify_customers_job",
    selection=AssetSelection.groups(CUSTOMERS_GROUP),
    description="Sync all customer tables",
    tags={
        "pipeline": PIPELINE_TAG,
        "team": TEAM_TAG,
    },
)


catalog_job = define_asset_job(
    name="shopify_catalog_job",
    selection=(
        AssetSelection.groups(PRODUCTS_GROUP)
        | AssetSelection.groups(COLLECTIONS_GROUP)
    ),
    description="Sync products + collections + bridge",
    tags={
        "pipeline": PIPELINE_TAG,
        "team": TEAM_TAG,
    },
)


shop_job = define_asset_job(
    name="shopify_shop_job",
    selection=AssetSelection.groups(SHOP_GROUP),
    description="Daily shop metadata refresh",
    tags={
        "pipeline": PIPELINE_TAG,
        "team": TEAM_TAG,
    },
)


full_sync_job = define_asset_job(
    name="shopify_full_sync_job",
    selection=(
        AssetSelection.groups(ORDERS_GROUP)
        | AssetSelection.groups(CUSTOMERS_GROUP)
        | AssetSelection.groups(PRODUCTS_GROUP)
        | AssetSelection.groups(COLLECTIONS_GROUP)
        | AssetSelection.groups(SHOP_GROUP)
    ),
    description="All Shopify assets",
    tags={
        "pipeline": PIPELINE_TAG,
        "team": TEAM_TAG,
    },
)


all_jobs = [
    orders_job,
    customers_job,
    catalog_job,
    shop_job,
    full_sync_job,
]
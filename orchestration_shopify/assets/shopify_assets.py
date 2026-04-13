"""Dagster software-defined assets for Shopify data pipeline.

Each asset wraps a specific dlt resource from the existing shopify_dlt package.
"""

import logging
from datetime import datetime, timezone

from dagster import (
    asset,
    AssetExecutionContext,
    MetadataValue,
    AssetKey,
    Output,
)

from ..resources.shopify_resource import ShopifyPipelineResource
from ..utils.constants import (
    SHOPIFY_GROUP,
    ORDERS_MAX_LAG_MINUTES,
    CUSTOMERS_MAX_LAG_MINUTES,
    PRODUCTS_MAX_LAG_MINUTES,
    COLLECTIONS_MAX_LAG_MINUTES,
    SHOP_MAX_LAG_MINUTES,
)

logger = logging.getLogger("dagster.shopify.assets")


# =============================================================================
# HELPER
# =============================================================================

def _build_metadata(shopify_pipeline, load_info) -> dict:
    """Build standard metadata dict for all assets."""
    metadata = shopify_pipeline.parse_load_info(load_info)
    metadata["last_sync"] = MetadataValue.text(
        datetime.now(timezone.utc).isoformat()
    )
    metadata["orders_max_lag_minutes"] = ORDERS_MAX_LAG_MINUTES
    metadata["customers_max_lag_minutes"] = CUSTOMERS_MAX_LAG_MINUTES
    metadata["products_max_lag_minutes"] = PRODUCTS_MAX_LAG_MINUTES
    metadata["collections_max_lag_minutes"] = COLLECTIONS_MAX_LAG_MINUTES
    metadata["shop_max_lag_minutes"] = SHOP_MAX_LAG_MINUTES
    return metadata


# =============================================================================
# SHOP METADATA — daily, full replace
# =============================================================================

@asset(
    name="shopify_shop",
    group_name=SHOPIFY_GROUP,
    description="Shop-level metadata: plan, features, currencies, settings. Full replace each run.",
    compute_kind="dlt",
    metadata={
        "dlt_resource": "shop",
        "write_disposition": "replace",
        "table": "shop",
    },
)
def shopify_shop_asset(
    context: AssetExecutionContext,
    shopify_pipeline: ShopifyPipelineResource,
) -> Output[None]:
    """Materialize shop metadata."""
    context.log.info("Starting shop metadata sync")

    pipeline, load_info = shopify_pipeline.run_resources(
        resource_names=["shop"],
    )

    metadata = _build_metadata(shopify_pipeline, load_info)
    context.log.info("Shop sync completed: %s", load_info)
    return Output(None, metadata=metadata)


# =============================================================================
# ORDERS — every 30 min, incremental merge
# =============================================================================

@asset(
    name="shopify_orders",
    group_name=SHOPIFY_GROUP,
    description=(
        "Order transactions including line items, refunds, discounts, "
        "shipping lines, and tax lines. Incremental merge on updatedAt."
    ),
    compute_kind="dlt",
    metadata={
        "dlt_resource": "orders",
        "write_disposition": "merge",
        "table": "order",
        "child_tables": [
            "order__line_items",
            "order__refunds",
            "order__discount_applications",
            "order__tax_lines",
            "order__shipping_lines",
        ],
    },
)
def shopify_orders_asset(
    context: AssetExecutionContext,
    shopify_pipeline: ShopifyPipelineResource,
) -> Output[None]:
    """Materialize orders with incremental loading."""
    context.log.info("Starting orders sync")

    pipeline, load_info = shopify_pipeline.run_resources(
        resource_names=["orders"],
    )

    metadata = _build_metadata(shopify_pipeline, load_info)
    context.log.info("Orders sync completed: %s", load_info)
    return Output(None, metadata=metadata)


# =============================================================================
# CUSTOMERS — every 6 hours, incremental merge
# =============================================================================

@asset(
    name="shopify_customers",
    group_name=SHOPIFY_GROUP,
    description=(
        "Customer accounts including tags. "
        "Incremental merge on updatedAt. PII fields are hashed."
    ),
    compute_kind="dlt",
    metadata={
        "dlt_resource": "customers",
        "write_disposition": "merge",
        "table": "customer",
        "child_tables": ["customer__tags"],
    },
)
def shopify_customers_asset(
    context: AssetExecutionContext,
    shopify_pipeline: ShopifyPipelineResource,
) -> Output[None]:
    """Materialize customers with incremental loading."""
    context.log.info("Starting customers sync")

    pipeline, load_info = shopify_pipeline.run_resources(
        resource_names=["customers"],
    )

    metadata = _build_metadata(shopify_pipeline, load_info)
    context.log.info("Customers sync completed: %s", load_info)
    return Output(None, metadata=metadata)


# =============================================================================
# PRODUCTS — every 12 hours, incremental merge
# =============================================================================

@asset(
    name="shopify_products",
    group_name=SHOPIFY_GROUP,
    description=(
        "Product catalog including variants, images, options, and tags. "
        "Incremental merge on updatedAt."
    ),
    compute_kind="dlt",
    metadata={
        "dlt_resource": "products",
        "write_disposition": "merge",
        "table": "product",
        "child_tables": [
            "product__variants",
            "product__images",
            "product__options",
            "product__tags",
        ],
    },
)
def shopify_products_asset(
    context: AssetExecutionContext,
    shopify_pipeline: ShopifyPipelineResource,
) -> Output[None]:
    """Materialize products with incremental loading."""
    context.log.info("Starting products sync")

    pipeline, load_info = shopify_pipeline.run_resources(
        resource_names=["products"],
    )

    metadata = _build_metadata(shopify_pipeline, load_info)
    context.log.info("Products sync completed: %s", load_info)
    return Output(None, metadata=metadata)


# =============================================================================
# COLLECTIONS — every 12 hours, incremental merge
# =============================================================================

@asset(
    name="shopify_collections",
    group_name=SHOPIFY_GROUP,
    description=(
        "Collection metadata including smart/custom rules. "
        "Incremental merge on updatedAt."
    ),
    compute_kind="dlt",
    metadata={
        "dlt_resource": "collections",
        "write_disposition": "merge",
        "table": "collection",
        "child_tables": ["collection__rules"],
    },
)
def shopify_collections_asset(
    context: AssetExecutionContext,
    shopify_pipeline: ShopifyPipelineResource,
) -> Output[None]:
    """Materialize collections with incremental loading."""
    context.log.info("Starting collections sync")

    pipeline, load_info = shopify_pipeline.run_resources(
        resource_names=["collections"],
    )

    metadata = _build_metadata(shopify_pipeline, load_info)
    context.log.info("Collections sync completed: %s", load_info)
    return Output(None, metadata=metadata)


# =============================================================================
# COLLECTION PRODUCTS BRIDGE — depends on collections, full replace
# =============================================================================

@asset(
    name="shopify_collection_products",
    group_name=SHOPIFY_GROUP,
    description=(
        "Current-state bridge between collections and products. "
        "Full replace each run. Runs after collections complete."
    ),
    compute_kind="dlt",
    deps=[AssetKey("shopify_collections")],
    metadata={
        "dlt_resource": "collection_products",
        "write_disposition": "replace",
        "table": "collection_product",
    },
)
def shopify_collection_products_asset(
    context: AssetExecutionContext,
    shopify_pipeline: ShopifyPipelineResource,
) -> Output[None]:
    """Materialize collection-product bridge (full snapshot)."""
    context.log.info("Starting collection_products sync (depends on collections)")

    pipeline, load_info = shopify_pipeline.run_resources(
        resource_names=["collection_products"],
    )

    metadata = _build_metadata(shopify_pipeline, load_info)
    context.log.info("Collection products sync completed: %s", load_info)
    return Output(None, metadata=metadata)
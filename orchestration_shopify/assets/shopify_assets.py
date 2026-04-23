"""Dagster assets — all tables with sub-groups, owners, dynamic kinds, and lineage."""

import logging
import os
from datetime import datetime, timezone

import dlt
from dagster import (
    asset,
    AssetExecutionContext,
    AssetKey,
    MetadataValue,
    Output,
)

from ..utils.constants import (
    ORDERS_GROUP,
    CUSTOMERS_GROUP,
    PRODUCTS_GROUP,
    COLLECTIONS_GROUP,
    SHOP_GROUP,
    ASSET_OWNER,
)

logger = logging.getLogger("dagster.shopify.assets")

PREFIX = ["shopify"]

# Dynamic kinds based on destination type
_DESTINATION = os.getenv("DESTINATION", "duckdb").lower()
DESTINATION_KIND = {
    "duckdb": "duckdb",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
}.get(_DESTINATION, "duckdb")

KINDS = {"dlt", "shopify",DESTINATION_KIND}


# =============================================================================
# HELPERS
# =============================================================================

def _get_pipeline():
    """Create dlt pipeline with current env config."""
    destination = os.getenv("DESTINATION", "duckdb").lower()

    if destination == "snowflake":
        from shopify_dlt.config import (
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_USER,
            SNOWFLAKE_PASSWORD,
            SNOWFLAKE_DATABASE,
            SNOWFLAKE_WAREHOUSE,
            SNOWFLAKE_ROLE,
        )

        conn = (
            f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}"
            f"@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}"
            f"?warehouse={SNOWFLAKE_WAREHOUSE}"
        )
        if SNOWFLAKE_ROLE:
            conn += f"&role={SNOWFLAKE_ROLE}"

        dest = dlt.destinations.snowflake(conn)

    elif destination == "bigquery":
        dest = dlt.destinations.bigquery()

    else:
        duckdb_path = os.getenv("DUCKDB_PATH", "data/duckdb/shopify.duckdb")
        os.makedirs(os.path.dirname(duckdb_path), exist_ok=True)
        dest = dlt.destinations.duckdb(duckdb_path)

    return dlt.pipeline(
        pipeline_name=os.getenv("PIPELINE_NAME", "shopify_dlt"),
        destination=dest,
        dataset_name=os.getenv("DATASET_NAME", "shopify_data"),
    )


def _run_dlt(context, resource_names):
    """Run dlt pipeline and return metadata."""
    from shopify_dlt.shopify_sources import shopify_source

    source = shopify_source().with_resources(*resource_names)
    pipeline = _get_pipeline()
    load_info = pipeline.run(source)

    metadata = {
        "load_id": MetadataValue.text(str(getattr(load_info, "load_id", "unknown"))),
        "destination": MetadataValue.text(os.getenv("DESTINATION", "duckdb")),
        "dataset": MetadataValue.text(os.getenv("DATASET_NAME", "shopify_data")),
        "last_sync": MetadataValue.text(datetime.now(timezone.utc).isoformat()),
    }

    context.log.info("dlt pipeline completed: %s", load_info)
    return metadata


def _child_metadata(parent):
    return {"status": "populated_by_parent", "parent": parent}


def _dep(name):
    """Build AssetKey with shopify prefix."""
    return AssetKey(["shopify", name])


# =============================================================================
# SHOP
# =============================================================================

@asset(
    name="shop",
    key_prefix=PREFIX,
    group_name=SHOP_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    description="Shop metadata — full replace each run",
    metadata={"write_disposition": "replace", "schedule": "daily 3AM UTC"},
)
def shop_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_run_dlt(context, ["shop"]))


# =============================================================================
# ORDERS
# =============================================================================

@asset(
    name="order",
    key_prefix=PREFIX,
    group_name=ORDERS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    description="Orders — incremental merge on updatedAt",
    metadata={"write_disposition": "merge", "schedule": "every 30 min"},
)
def order_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_run_dlt(context, ["orders"]))


@asset(
    name="order__line_items",
    key_prefix=PREFIX,
    group_name=ORDERS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    deps=[_dep("order")],
    description="Order line items",
)
def order_line_items_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_child_metadata("order"))


@asset(
    name="order__line_items__tax_lines",
    key_prefix=PREFIX,
    group_name=ORDERS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    deps=[_dep("order__line_items")],
    description="Tax lines per line item",
)
def order_li_tax_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_child_metadata("order__line_items"))


@asset(
    name="order__line_items__discount_allocations",
    key_prefix=PREFIX,
    group_name=ORDERS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    deps=[_dep("order__line_items")],
    description="Discount allocations per line item",
)
def order_li_discounts_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_child_metadata("order__line_items"))


@asset(
    name="order__line_items__custom_attributes",
    key_prefix=PREFIX,
    group_name=ORDERS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    deps=[_dep("order__line_items")],
    description="Custom attributes per line item",
)
def order_li_attrs_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_child_metadata("order__line_items"))


@asset(
    name="order__refunds",
    key_prefix=PREFIX,
    group_name=ORDERS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    deps=[_dep("order")],
    description="Order refunds",
)
def order_refunds_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_child_metadata("order"))


@asset(
    name="order__refunds__refund_line_items",
    key_prefix=PREFIX,
    group_name=ORDERS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    deps=[_dep("order__refunds")],
    description="Refund line items",
)
def order_refund_li_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_child_metadata("order__refunds"))


@asset(
    name="order__refunds__order_adjustments",
    key_prefix=PREFIX,
    group_name=ORDERS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    deps=[_dep("order__refunds")],
    description="Order adjustments on refunds",
)
def order_refund_adj_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_child_metadata("order__refunds"))


@asset(
    name="order__discount_applications",
    key_prefix=PREFIX,
    group_name=ORDERS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    deps=[_dep("order")],
    description="Order-level discount applications",
)
def order_discounts_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_child_metadata("order"))


@asset(
    name="order__tax_lines",
    key_prefix=PREFIX,
    group_name=ORDERS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    deps=[_dep("order")],
    description="Order-level tax lines",
)
def order_tax_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_child_metadata("order"))


@asset(
    name="order__shipping_lines",
    key_prefix=PREFIX,
    group_name=ORDERS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    deps=[_dep("order")],
    description="Order shipping lines",
)
def order_shipping_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_child_metadata("order"))


@asset(
    name="order__shipping_lines__tax_lines",
    key_prefix=PREFIX,
    group_name=ORDERS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    deps=[_dep("order__shipping_lines")],
    description="Tax lines per shipping line",
)
def order_shipping_tax_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_child_metadata("order__shipping_lines"))


# =============================================================================
# CUSTOMERS
# =============================================================================

@asset(
    name="customer",
    key_prefix=PREFIX,
    group_name=CUSTOMERS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    description="Customers — incremental merge on updatedAt. PII hashed.",
    metadata={"write_disposition": "merge", "schedule": "every 6 hours"},
)
def customer_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_run_dlt(context, ["customers"]))


@asset(
    name="customer__tags",
    key_prefix=PREFIX,
    group_name=CUSTOMERS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    deps=[_dep("customer")],
    description="Customer tags",
)
def customer_tags_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_child_metadata("customer"))


# =============================================================================
# PRODUCTS
# =============================================================================

@asset(
    name="product",
    key_prefix=PREFIX,
    group_name=PRODUCTS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    description="Products — incremental merge on updatedAt",
    metadata={"write_disposition": "merge", "schedule": "every 12 hours"},
)
def product_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_run_dlt(context, ["products"]))


@asset(
    name="product__variants",
    key_prefix=PREFIX,
    group_name=PRODUCTS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    deps=[_dep("product")],
    description="Product variants (SKUs, prices, inventory)",
)
def product_variants_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_child_metadata("product"))


@asset(
    name="product__variants__selected_options",
    key_prefix=PREFIX,
    group_name=PRODUCTS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    deps=[_dep("product__variants")],
    description="Selected options per variant",
)
def product_var_options_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_child_metadata("product__variants"))


@asset(
    name="product__images",
    key_prefix=PREFIX,
    group_name=PRODUCTS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    deps=[_dep("product")],
    description="Product images",
)
def product_images_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_child_metadata("product"))


@asset(
    name="product__options",
    key_prefix=PREFIX,
    group_name=PRODUCTS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    deps=[_dep("product")],
    description="Product options (Color, Size, etc.)",
)
def product_options_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_child_metadata("product"))


@asset(
    name="product__options__option_values",
    key_prefix=PREFIX,
    group_name=PRODUCTS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    deps=[_dep("product__options")],
    description="Option values",
)
def product_opt_values_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_child_metadata("product__options"))


@asset(
    name="product__tags",
    key_prefix=PREFIX,
    group_name=PRODUCTS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    deps=[_dep("product")],
    description="Product tags",
)
def product_tags_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_child_metadata("product"))


# =============================================================================
# COLLECTIONS
# =============================================================================

@asset(
    name="collection",
    key_prefix=PREFIX,
    group_name=COLLECTIONS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    description="Collections — incremental merge on updatedAt",
    metadata={"write_disposition": "merge", "schedule": "every 12 hours"},
)
def collection_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_run_dlt(context, ["collections"]))


@asset(
    name="collection__rules",
    key_prefix=PREFIX,
    group_name=COLLECTIONS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    deps=[_dep("collection")],
    description="Smart collection rules",
)
def collection_rules_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_child_metadata("collection"))


@asset(
    name="collection_product",
    key_prefix=PREFIX,
    group_name=COLLECTIONS_GROUP,
    kinds=KINDS,
    owners=[ASSET_OWNER],
    deps=[_dep("collection")],
    description="Collection-product bridge — full replace",
    metadata={"write_disposition": "replace", "schedule": "every 12 hours"},
)
def collection_product_asset(context: AssetExecutionContext) -> Output[None]:
    return Output(None, metadata=_run_dlt(context, ["collection_products"]))
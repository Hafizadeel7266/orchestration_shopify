"""Pipeline runner - executes Shopify data pipeline."""

import dlt
import logging
from typing import Iterable, Optional

from .config import (
    FETCH_LAST_DAYS,
    INCREMENTAL_LOOKBACK_DAYS,
    DEFAULT_CHUNK_DAYS,
    PAGE_SIZE,
    DUCKDB_PATH,
    PIPELINE_NAME,
    DATASET_NAME,
    DESTINATION,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_ROLE,
)
from .shopify_sources import shopify_source

logger = logging.getLogger("shopify_pipeline.runner")


def _build_destination(destination: str, destination_path: str = None):
    """Build dlt destination object.

    Args:
        destination: One of 'duckdb', 'bigquery', 'snowflake'
        destination_path: File path for DuckDB (ignored for cloud)

    Returns:
        dlt destination object
    """
    if destination == "snowflake":
        connection_parts = [
            f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}",
            f"@{SNOWFLAKE_ACCOUNT}/{SNOWFLAKE_DATABASE}",
            f"?warehouse={SNOWFLAKE_WAREHOUSE}",
        ]
        if SNOWFLAKE_ROLE:
            connection_parts.append(f"&role={SNOWFLAKE_ROLE}")

        logger.info(
            "Using Snowflake: account=%s, db=%s, wh=%s",
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_DATABASE,
            SNOWFLAKE_WAREHOUSE,
        )
        return dlt.destinations.snowflake("".join(connection_parts))

    if destination == "bigquery":
        logger.info("Using BigQuery destination")
        return dlt.destinations.bigquery()

    logger.info("Using DuckDB: %s", destination_path)
    return dlt.destinations.duckdb(destination_path)


def run_pipeline(
    shop_url: str = None,
    access_token: str = None,
    initial_load_past_days: int = FETCH_LAST_DAYS,
    lookback_days: int = INCREMENTAL_LOOKBACK_DAYS,
    chunk_days: int = DEFAULT_CHUNK_DAYS,
    backfill_days: int = 0,
    page_size: int = PAGE_SIZE,
    max_rows: int = 0,
    start_date_override=None,
    end_date_override=None,
    destination: str = DESTINATION,
    destination_path: str = DUCKDB_PATH,
    pipeline_name: str = PIPELINE_NAME,
    dataset_name: str = DATASET_NAME,
    dev_mode: bool = False,
    resource_names: Optional[Iterable[str]] = None,
):
    """
    Run Shopify pipeline - ALL data into ONE dataset.

    Args:
        destination: "duckdb", "bigquery", or "snowflake"
        destination_path: Path for duckdb file (ignored for cloud destinations)
        resource_names: Optional resource names to select via with_resources()

    Returns:
        tuple: (pipeline, load_info)
    """

    logger.info("Shopify pipeline starting")

    source = shopify_source(
        shop_url=shop_url,
        access_token=access_token,
        initial_load_past_days=initial_load_past_days,
        lookback_days=lookback_days,
        chunk_days=chunk_days,
        backfill_days=backfill_days,
        page_size=page_size,
        max_rows=max_rows,
        start_date_override=start_date_override,
        end_date_override=end_date_override,
    )
    if resource_names:
        source = source.with_resources(*tuple(resource_names))

    dest = _build_destination(destination, destination_path)

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dest,
        dataset_name=dataset_name,
        dev_mode=dev_mode,
    )

    logger.info(f"Running pipeline: destination={destination}, dataset={dataset_name}")

    try:
        load_info = pipeline.run(source)
        logger.info(f"Pipeline completed: {load_info}")
        return pipeline, load_info
    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise


def run_orders_only(**kwargs):
    """Run only orders data."""
    logger.info("Running orders-only pipeline")
    return run_pipeline(resource_names=("orders",), **kwargs)


def run_customers_only(**kwargs):
    """Run only customers data."""
    logger.info("Running customers-only pipeline")
    return run_pipeline(resource_names=("customers",), **kwargs)


def run_shop_only(**kwargs):
    """Run only shop data."""
    logger.info("Running shop-only pipeline")
    return run_pipeline(resource_names=("shop",), **kwargs)


def run_products_only(**kwargs):
    """Run only product catalog parent/child tables."""
    logger.info("Running products-only pipeline")
    return run_pipeline(resource_names=("products",), **kwargs)


def run_collections_only(**kwargs):
    """Run collection metadata plus membership bridge."""
    logger.info("Running collections-only pipeline")
    return run_pipeline(resource_names=("collections", "collection_products"), **kwargs)


def run_catalog_only(**kwargs):
    """Run product and collection catalog resources only."""
    logger.info("Running catalog-only pipeline")
    return run_pipeline(
        resource_names=("products", "collections", "collection_products"),
        **kwargs,
    )
#!/usr/bin/env python3
"""Run a fixed historical Shopify window, then a real incremental replay.

Quick commands:
    # Default orders replay into DuckDB
    python -m dlt_functions.shopify_dlt.scripts.run_incremental_replay --mode orders

    # Replay products only into DuckDB
    python -m dlt_functions.shopify_dlt.scripts.run_incremental_replay --mode products

    # Replay collections and collection membership into DuckDB
    python -m dlt_functions.shopify_dlt.scripts.run_incremental_replay --mode collections

    # Replay the full catalog shape into DuckDB
    python -m dlt_functions.shopify_dlt.scripts.run_incremental_replay --mode catalog

    # Replay all Shopify resources into DuckDB
    python -m dlt_functions.shopify_dlt.scripts.run_incremental_replay --mode all

    # Replay orders into BigQuery with a fresh dataset + pipeline state
    python -m dlt_functions.shopify_dlt.scripts.run_incremental_replay --mode orders --destination bigquery --pipeline-name shopify_incremental_replay --dataset-name shopify_incremental_replay_20260320

    # Adjust the fixed historical window and the real incremental lookback
    python -m dlt_functions.shopify_dlt.scripts.run_incremental_replay --mode orders --historical-start-offset-days 14 --historical-end-offset-days 2 --incremental-lookback-days 2
"""

import argparse
import logging
import sys
from typing import Optional, Tuple

from dlt.common import pendulum

from dlt_functions.shopify_dlt.config import (
    DATASET_NAME,
    DEFAULT_CHUNK_DAYS,
    DUCKDB_PATH,
    FETCH_LAST_DAYS,
    PAGE_SIZE,
    PIPELINE_NAME,
)
from dlt_functions.shopify_dlt.runner import run_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def compute_historical_window(
    now: pendulum.DateTime,
    historical_start_offset_days: int,
    historical_end_offset_days: int,
) -> Tuple[pendulum.DateTime, pendulum.DateTime]:
    """Build the fixed UTC historical window."""
    if historical_start_offset_days <= historical_end_offset_days:
        raise ValueError(
            "historical_start_offset_days must be greater than historical_end_offset_days"
        )
    if historical_end_offset_days < 0:
        raise ValueError("historical_end_offset_days must be non-negative")

    start_date = now.subtract(days=historical_start_offset_days).start_of("day")
    end_date = now.subtract(days=historical_end_offset_days).end_of("day")
    return start_date, end_date


MODE_RESOURCES = {
    "orders": ("orders",),
    "customers": ("customers",),
    "products": ("products",),
    "collections": ("collections", "collection_products"),
    "catalog": ("products", "collections", "collection_products"),
    "shop": ("shop",),
    "all": None,  # None = no filtering, run everything
}


def run_replay(
    mode: str = "orders",
    historical_start_offset_days: int = 10,
    historical_end_offset_days: int = 2,
    incremental_lookback_days: int = 2,
    shop_url: Optional[str] = None,
    access_token: Optional[str] = None,
    initial_load_past_days: int = FETCH_LAST_DAYS,
    chunk_days: int = DEFAULT_CHUNK_DAYS,
    page_size: int = PAGE_SIZE,
    max_rows: int = 0,
    destination: str = "duckdb",
    destination_path: str = DUCKDB_PATH,
    pipeline_name: str = PIPELINE_NAME,
    dataset_name: str = DATASET_NAME,
    dev_mode: bool = False,
):
    """Run the two-phase Shopify replay flow."""
    now = pendulum.now("UTC")
    historical_start, historical_end = compute_historical_window(
        now,
        historical_start_offset_days,
        historical_end_offset_days,
    )
    resource_names = MODE_RESOURCES.get(mode)

    logger.info("=" * 60)
    logger.info("SHOPIFY INCREMENTAL REPLAY")
    logger.info("=" * 60)
    logger.info("Mode: %s", mode)
    logger.info(
        "Run 1 fixed window: %s -> %s",
        historical_start.to_iso8601_string(),
        historical_end.to_iso8601_string(),
    )
    logger.info("Run 2 incremental lookback: %s day(s)", incremental_lookback_days)
    logger.info("Pipeline: %s", pipeline_name)
    logger.info("Dataset: %s", dataset_name)

    logger.info("Starting Run 1: fixed historical window")
    _, historical_info = run_pipeline(
        shop_url=shop_url,
        access_token=access_token,
        initial_load_past_days=initial_load_past_days,
        lookback_days=0,
        chunk_days=chunk_days,
        backfill_days=0,
        page_size=page_size,
        max_rows=max_rows,
        start_date_override=historical_start.to_iso8601_string(),
        end_date_override=historical_end.to_iso8601_string(),
        destination=destination,
        destination_path=destination_path,
        pipeline_name=pipeline_name,
        dataset_name=dataset_name,
        dev_mode=dev_mode,
        resource_names=resource_names,
    )
    logger.info("Run 1 completed: %s", historical_info)

    logger.info("Starting Run 2: state-driven incremental replay")
    _, incremental_info = run_pipeline(
        shop_url=shop_url,
        access_token=access_token,
        initial_load_past_days=initial_load_past_days,
        lookback_days=incremental_lookback_days,
        chunk_days=chunk_days,
        backfill_days=0,
        page_size=page_size,
        max_rows=max_rows,
        destination=destination,
        destination_path=destination_path,
        pipeline_name=pipeline_name,
        dataset_name=dataset_name,
        dev_mode=dev_mode,
        resource_names=resource_names,
    )
    logger.info("Run 2 completed: %s", incremental_info)

    return historical_info, incremental_info


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI parser."""
    parser = argparse.ArgumentParser(
        description="Run a Shopify fixed-window + incremental replay"
    )
    parser.add_argument(
        "--mode",
        choices=["all", "orders", "customers", "products", "collections", "catalog", "shop"],
        default="orders",
        help="Resource mode to run (default: orders)",
    )
    parser.add_argument(
        "--historical-start-offset-days",
        type=int,
        default=10,
        help="Historical fixed-window start offset in UTC days ago",
    )
    parser.add_argument(
        "--historical-end-offset-days",
        type=int,
        default=2,
        help="Historical fixed-window end offset in UTC days ago",
    )
    parser.add_argument(
        "--incremental-lookback-days",
        type=int,
        default=2,
        help="Lookback days for the real incremental second run",
    )
    parser.add_argument("--shop-url")
    parser.add_argument("--access-token")
    parser.add_argument("--initial-load-past-days", type=int, default=FETCH_LAST_DAYS)
    parser.add_argument("--chunk-days", type=int, default=DEFAULT_CHUNK_DAYS)
    parser.add_argument("--page-size", type=int, default=PAGE_SIZE)
    parser.add_argument("--max-rows", type=int, default=0)
    parser.add_argument(
        "--destination",
        choices=["duckdb", "bigquery"],
        default="duckdb",
    )
    parser.add_argument("--destination-path", default=DUCKDB_PATH)
    parser.add_argument("--pipeline-name", default=PIPELINE_NAME)
    parser.add_argument("--dataset-name", default=DATASET_NAME)
    parser.add_argument("--dev-mode", action="store_true")
    return parser


def main() -> int:
    """CLI entrypoint."""
    args = build_parser().parse_args()
    run_replay(
        mode=args.mode,
        historical_start_offset_days=args.historical_start_offset_days,
        historical_end_offset_days=args.historical_end_offset_days,
        incremental_lookback_days=args.incremental_lookback_days,
        shop_url=args.shop_url,
        access_token=args.access_token,
        initial_load_past_days=args.initial_load_past_days,
        chunk_days=args.chunk_days,
        page_size=args.page_size,
        max_rows=args.max_rows,
        destination=args.destination,
        destination_path=args.destination_path,
        pipeline_name=args.pipeline_name,
        dataset_name=args.dataset_name,
        dev_mode=args.dev_mode,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

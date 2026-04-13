#!/usr/bin/env python
"""Main entry point for Shopify DLT Pipeline."""

import argparse
import os
from dotenv import load_dotenv

try:
    from .runner import (
        run_pipeline,
        run_orders_only,
        run_customers_only,
        run_products_only,
        run_collections_only,
        run_catalog_only,
        run_shop_only,
    )
except ImportError:
    from runner import (
        run_pipeline,
        run_orders_only,
        run_customers_only,
        run_products_only,
        run_collections_only,
        run_catalog_only,
        run_shop_only,
    )

load_dotenv()

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Shopify DLT Pipeline")
    
    parser.add_argument(
        "--mode",
        choices=["all", "orders", "customers", "products", "collections", "catalog", "shop"],
        default="all",
        help="Data to load (default: all)"
    )
    
    parser.add_argument(
        "--backfill-days",
        type=int,
        default=0,
        help="Days to backfill (0 = incremental)"
    )
    
    parser.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Records per API call"
    )
    
    parser.add_argument(
        "--start-date",
        help="Explicit UTC start datetime (ISO-8601)"
    )
    
    parser.add_argument(
        "--end-date",
        help="Explicit UTC end datetime (ISO-8601)"
    )
    
    parser.add_argument(
        "--destination",
        choices=["duckdb", "bigquery", "snowflake"],
        default=None,
        help="Override destination"
    )

    args = parser.parse_args()

    kwargs = {
        "backfill_days": args.backfill_days,
        "page_size": args.page_size,
        "start_date_override": args.start_date,
        "end_date_override": args.end_date,
    }

    if args.destination:
        kwargs["destination"] = args.destination

    if args.mode == "orders":
        run_orders_only(**kwargs)
    elif args.mode == "customers":
        run_customers_only(**kwargs)
    elif args.mode == "products":
        run_products_only(**kwargs)
    elif args.mode == "collections":
        run_collections_only(**kwargs)
    elif args.mode == "catalog":
        run_catalog_only(**kwargs)
    elif args.mode == "shop":
        run_shop_only(**kwargs)
    else:
        run_pipeline(**kwargs)


if __name__ == "__main__":
    main()
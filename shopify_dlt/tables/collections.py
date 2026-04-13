"""Collection table resources for Shopify GraphQL catalog sync."""

import logging
from datetime import datetime, timezone

import dlt
import pendulum

from ..helpers.dates import get_end_date, get_start_date_with_backfill
from ..helpers.transforms import parse_gids
from ..queries.collections import (
    ALL_COLLECTION_IDS_QUERY,
    COLLECTION_PRODUCTS_QUERY,
    COLLECTIONS_QUERY,
)
from ..schemas.schemas import COLLECTION_HINTS, COLLECTION_PRODUCT_COLUMNS

logger = logging.getLogger("shopify_pipeline.collections")


def transform_collection(node: dict) -> dict:
    """Normalize a raw collection GraphQL node for DLT auto-flatten."""
    rule_set = node.pop("ruleSet", None)
    node["collectionType"] = "smart" if rule_set else "custom"
    node["ruleSetAppliedDisjunctively"] = None
    node["rules"] = []

    if isinstance(rule_set, dict):
        node["ruleSetAppliedDisjunctively"] = rule_set.get("appliedDisjunctively")
        node["rules"] = [
            {
                "collectionId": None,  # set after GID parsing
                "index": idx,
                "column": rule.get("column"),
                "relation": rule.get("relation"),
                "condition": rule.get("condition"),
            }
            for idx, rule in enumerate(rule_set.get("rules") or [])
        ]

    parse_gids(node)

    collection_id = node.get("id")
    for rule in node.get("rules", []):
        rule["collectionId"] = collection_id

    node["_loaded_at"] = datetime.now(timezone.utc).isoformat()
    return node


def fetch_collections(graphql_client, start_date, end_date, chunk_days=7, page_size=100, max_rows=0):
    """Fetch collections from Shopify GraphQL API with date chunking and pagination."""
    current_start = pendulum.instance(start_date) if not isinstance(start_date, pendulum.DateTime) else start_date
    end_dt = pendulum.instance(end_date) if not isinstance(end_date, pendulum.DateTime) else end_date
    row_count = 0

    while current_start < end_dt:
        chunk_end = min(current_start.add(days=chunk_days), end_dt)
        query_filter = (
            f"updated_at:>='{current_start.to_iso8601_string()}' "
            f"AND updated_at:<='{chunk_end.to_iso8601_string()}'"
        )

        logger.info(
            "Fetching collections: %s to %s",
            current_start.to_date_string(),
            chunk_end.to_date_string(),
        )

        cursor = None
        has_next = True

        while has_next:
            variables = {"query": query_filter, "first": page_size}
            if cursor:
                variables["after"] = cursor

            response = graphql_client(COLLECTIONS_QUERY, variables)
            if not response or "data" not in response:
                logger.error("No data in GraphQL response")
                break

            collections_data = response["data"].get("collections", {})
            edges = collections_data.get("edges", [])
            page_info = collections_data.get("pageInfo", {})

            for edge in edges:
                node = edge.get("node", {})
                yield transform_collection(node)
                row_count += 1
                if max_rows and row_count >= max_rows:
                    logger.info("Reached max_rows limit (%s), stopping", max_rows)
                    return

            has_next = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")

        current_start = chunk_end


def fetch_collection_products(graphql_client, page_size=100, max_rows=0):
    """Fetch the full current-state collection membership bridge."""
    collection_cursor = None
    has_next_collections = True
    row_count = 0

    while has_next_collections:
        response = graphql_client(
            ALL_COLLECTION_IDS_QUERY,
            {"first": page_size, "after": collection_cursor},
        )
        collections_data = response.get("data", {}).get("collections", {})
        collection_edges = collections_data.get("edges", [])
        collection_page_info = collections_data.get("pageInfo", {})

        for edge in collection_edges:
            collection_gid = edge.get("node", {}).get("id")
            if not collection_gid:
                continue

            product_cursor = None
            has_next_products = True

            while has_next_products:
                product_response = graphql_client(
                    COLLECTION_PRODUCTS_QUERY,
                    {"id": collection_gid, "first": page_size, "after": product_cursor},
                )
                collection = product_response.get("data", {}).get("collection", {})
                products = collection.get("products", {})

                collection_id = parse_gids({"id": collection_gid}).get("id")
                for product in products.get("nodes", []):
                    parse_gids(product)
                    yield {
                        "collectionId": collection_id,
                        "productId": product.get("id"),
                        "_loaded_at": datetime.now(timezone.utc).isoformat(),
                    }
                    row_count += 1
                    if max_rows and row_count >= max_rows:
                        logger.info("Reached max_rows limit (%s), stopping bridge snapshot", max_rows)
                        return

                product_page_info = products.get("pageInfo", {})
                has_next_products = product_page_info.get("hasNextPage", False)
                product_cursor = product_page_info.get("endCursor")

        has_next_collections = collection_page_info.get("hasNextPage", False)
        collection_cursor = collection_page_info.get("endCursor")


@dlt.resource(
    name="collections",
    primary_key=["shopId", "id"],
    write_disposition="merge",
    table_name="collection",
    **COLLECTION_HINTS,
)
def collections_table(
    graphql_client,
    updated_at=dlt.sources.incremental("updatedAt", initial_value="2020-01-01T00:00:00Z"),
    chunk_days=7,
    start_date_override=None,
    end_date_override=None,
    backfill_days=0,
    page_size=100,
    max_rows=0,
):
    """DLT resource for Shopify collections with incremental loading."""
    start_date = get_start_date_with_backfill(
        updated_at,
        backfill_days,
        start_date_override=start_date_override,
    )
    end_date = get_end_date(end_date_override)

    logger.info("Collections: %s to %s (backfill=%sd)", start_date, end_date, backfill_days)
    yield from fetch_collections(graphql_client, start_date, end_date, chunk_days, page_size, max_rows)


@dlt.resource(
    name="collection_products",
    primary_key=["shopId", "collectionId", "productId"],
    write_disposition="replace",
    table_name="collection_product",
    columns=COLLECTION_PRODUCT_COLUMNS,
)
def collection_products_table(
    graphql_client,
    page_size=100,
    max_rows=0,
):
    """DLT resource for current-state Shopify collection membership."""
    logger.info("Fetching current-state collection membership snapshot")
    yield from fetch_collection_products(graphql_client, page_size=page_size, max_rows=max_rows)


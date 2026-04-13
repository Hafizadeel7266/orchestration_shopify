"""Product table resource for Shopify GraphQL catalog sync."""

import logging
from datetime import datetime, timezone

import dlt
import pendulum

from ..helpers.dates import get_end_date, get_start_date_with_backfill
from ..helpers.transforms import parse_gids
from ..queries.products import PRODUCTS_QUERY
from ..schemas.schemas import PRODUCT_HINTS

logger = logging.getLogger("shopify_pipeline.products")


def _unwrap_connection(node: dict, field: str, parent_label: str) -> None:
    container = node.get(field)
    if not isinstance(container, dict):
        return
    page_info = container.get("pageInfo", {})
    if page_info.get("hasNextPage"):
        logger.warning("%s %s has >250 %s; data truncated", parent_label, node.get("id"), field)
    node[field] = container.get("nodes", [])


def transform_product(node: dict) -> dict:
    """Normalize a raw product GraphQL node for DLT auto-flatten."""
    _unwrap_connection(node, "images", "Product")
    _unwrap_connection(node, "variants", "Product")

    parse_gids(node)

    product_id = node.get("id")

    tags = node.get("tags")
    if isinstance(tags, list):
        node["tags"] = [
            {"productId": product_id, "index": idx, "value": tag}
            for idx, tag in enumerate(tags)
        ]

    for image in node.get("images", []):
        image["productId"] = product_id

    for option in node.get("options", []):
        option["productId"] = product_id
        for idx, option_value in enumerate(option.get("optionValues", [])):
            option_value["productOptionId"] = option.get("id")
            option_value["index"] = idx

    for variant in node.get("variants", []):
        variant["productId"] = product_id
        for idx, selected_option in enumerate(variant.get("selectedOptions", [])):
            selected_option["variantId"] = variant.get("id")
            selected_option["index"] = idx

    node["_loaded_at"] = datetime.now(timezone.utc).isoformat()
    return node


def fetch_products(graphql_client, start_date, end_date, chunk_days=7, page_size=100, max_rows=0):
    """Fetch products from Shopify GraphQL API with date chunking and pagination."""
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
            "Fetching products: %s to %s",
            current_start.to_date_string(),
            chunk_end.to_date_string(),
        )

        cursor = None
        has_next = True

        while has_next:
            variables = {"query": query_filter, "first": page_size}
            if cursor:
                variables["after"] = cursor

            response = graphql_client(PRODUCTS_QUERY, variables)
            if not response or "data" not in response:
                logger.error("No data in GraphQL response")
                break

            products_data = response["data"].get("products", {})
            edges = products_data.get("edges", [])
            page_info = products_data.get("pageInfo", {})

            for edge in edges:
                node = edge.get("node", {})
                yield transform_product(node)
                row_count += 1
                if max_rows and row_count >= max_rows:
                    logger.info("Reached max_rows limit (%s), stopping", max_rows)
                    return

            has_next = page_info.get("hasNextPage", False)
            cursor = page_info.get("endCursor")

        current_start = chunk_end


@dlt.resource(
    name="products",
    primary_key=["shopId", "id"],
    write_disposition="merge",
    table_name="product",
    **PRODUCT_HINTS,
)
def products_table(
    graphql_client,
    updated_at=dlt.sources.incremental("updatedAt", initial_value="2020-01-01T00:00:00Z"),
    chunk_days=7,
    start_date_override=None,
    end_date_override=None,
    backfill_days=0,
    page_size=100,
    max_rows=0,
):
    """DLT resource for Shopify products with incremental loading."""
    start_date = get_start_date_with_backfill(
        updated_at,
        backfill_days,
        start_date_override=start_date_override,
    )
    end_date = get_end_date(end_date_override)

    logger.info("Products: %s to %s (backfill=%sd)", start_date, end_date, backfill_days)
    yield from fetch_products(graphql_client, start_date, end_date, chunk_days, page_size, max_rows)

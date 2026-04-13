"""Single source for all Shopify data - loads into one dataset."""

import dlt
import logging
from dlt.common import pendulum

from .config import (
    FETCH_LAST_DAYS,
    INCREMENTAL_LOOKBACK_DAYS,
    DEFAULT_CHUNK_DAYS,
    PAGE_SIZE,
    SHOPIFY_SHOP_URL,
    SHOPIFY_ACCESS_TOKEN,
)
from .helpers.request_utils import create_graphql_client
from .helpers.parsers import parse_shopify_id
from .helpers.dates import validate_backfill_days
from .tables.orders import orders_table
from .tables.customers import customers_table
from .tables.products import products_table
from .tables.collections import collections_table, collection_products_table
from .tables.shop import shop_table

logger = logging.getLogger("shopify_pipeline.sources")


@dlt.source
def shopify_source(
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
):
    """
    Single source for ALL Shopify data - loads into ONE dataset.

    All resources are registered unconditionally. Use .with_resources()
    on the returned source to select which ones to run.

    Args:
        shop_url: Shopify store URL
        access_token: Shopify access token
        initial_load_past_days: Days to load on first run
        lookback_days: Days to look back for updates
        chunk_days: Days per API call chunk
        backfill_days: Days to backfill (0 = incremental)
        page_size: Records per page
        max_rows: Max rows per resource (0 = unlimited)
        start_date_override: Explicit start datetime
        end_date_override: Explicit end datetime
    """

    shop_url = shop_url or SHOPIFY_SHOP_URL
    access_token = access_token or SHOPIFY_ACCESS_TOKEN
    backfill_days = validate_backfill_days(backfill_days)

    mode = 'backfill' if backfill_days > 0 else 'incremental'
    logger.info(f"Shopify source: shop={shop_url}, mode={mode}")

    graphql_client = create_graphql_client(shop_url, access_token)

    # Fetch shop identity once — inject into every row for multi-shop clustering
    shop_identity, plan_is_plus = _fetch_shop_identity(graphql_client)
    if page_size == PAGE_SIZE and plan_is_plus:
        page_size = 250
    graphql_client.configure_rate_profile(plan_is_plus=plan_is_plus, page_size_cap=page_size)
    logger.info(
        "Shopify plan detected: plus=%s, requested_page_size=%s",
        plan_is_plus,
        page_size,
    )
    logger.info(f"Shop identity: id={shop_identity['shopId']}")

    def _inject_shop_id(row):
        row.update(shop_identity)
        _inject_into_children(row, shop_identity)
        return row

    incremental_kwargs = dict(
        chunk_days=chunk_days,
        backfill_days=backfill_days,
        page_size=page_size,
        max_rows=max_rows,
        start_date_override=start_date_override,
        end_date_override=end_date_override,
    )

    def _make_updated_at():
        return dlt.sources.incremental(
            "updatedAt",
            initial_value=pendulum.now().subtract(days=initial_load_past_days).to_iso8601_string(),
            lag=lookback_days,
        )

    return [
        orders_table(
            graphql_client=graphql_client,
            shop_id=shop_identity["shopId"],
            updated_at=_make_updated_at(),
            **incremental_kwargs,
        ).add_map(_inject_shop_id),
        customers_table(
            graphql_client=graphql_client,
            updated_at=_make_updated_at(),
            **incremental_kwargs,
        ).add_map(_inject_shop_id),
        products_table(
            graphql_client=graphql_client,
            updated_at=_make_updated_at(),
            **incremental_kwargs,
        ).add_map(_inject_shop_id),
        collections_table(
            graphql_client=graphql_client,
            updated_at=_make_updated_at(),
            **incremental_kwargs,
        ).add_map(_inject_shop_id),
        collection_products_table(
            graphql_client=graphql_client,
            page_size=page_size,
            max_rows=max_rows,
        ).add_map(_inject_shop_id),
        shop_table(graphql_client),
    ]


def _inject_into_children(obj, identity):
    """Recursively inject identity fields into all nested dicts inside lists."""
    for val in obj.values():
        if isinstance(val, list):
            for item in val:
                if isinstance(item, dict):
                    item.update(identity)
                    _inject_into_children(item, identity)


SHOP_IDENTITY_QUERY = """
query { shop { id name myshopifyDomain plan { shopifyPlus } } }
"""


def _fetch_shop_identity(graphql_client):
    """Fetch shop id for multi-shop clustering."""
    response = graphql_client(SHOP_IDENTITY_QUERY)
    shop = response.get('data', {}).get('shop', {})
    shop_id = parse_shopify_id(shop.get('id'))
    return {
        'shopId': shop_id,
    }, bool(shop.get('plan', {}).get('shopifyPlus'))

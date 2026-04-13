"""Customer table resource -- yields raw GraphQL nodes for DLT auto-flatten."""
import logging
from datetime import datetime, timezone

import dlt
import pendulum

from ..helpers.dates import get_end_date, get_start_date_with_backfill
from ..helpers.transforms import parse_gids, hash_pii_fields, CUSTOMER_PII_PATHS
from ..queries.customers import CUSTOMERS_QUERY
from ..schemas.schemas import CUSTOMER_HINTS

logger = logging.getLogger("shopify_pipeline.customers")


def transform_customer(node):
    """Apply minimal transforms to raw GraphQL customer node."""
    # Flatten defaultEmailAddress
    email_obj = node.pop('defaultEmailAddress', None)
    if isinstance(email_obj, dict):
        node['email'] = email_obj.get('emailAddress')
        node['emailMarketingConsentState'] = email_obj.get('marketingState')
        node['emailMarketingOptInLevel'] = email_obj.get('marketingOptInLevel')
        node['emailMarketingConsentUpdatedAt'] = email_obj.get('marketingUpdatedAt')

    # Flatten defaultPhoneNumber
    phone_obj = node.pop('defaultPhoneNumber', None)
    if isinstance(phone_obj, dict):
        node['phone'] = phone_obj.get('phoneNumber')

    # Flatten amountSpent
    amt = node.pop('amountSpent', None)
    if isinstance(amt, dict):
        node['totalSpent'] = amt.get('amount')
        node['totalSpentCurrency'] = amt.get('currencyCode')

    # Parse all GIDs
    parse_gids(node)

    # Convert tags from list of strings to list of dicts with FKs
    customer_id = node.get('id')
    tags = node.get('tags')
    if isinstance(tags, list):
        node['tags'] = [
            {'customerId': customer_id, 'index': idx, 'value': tag}
            for idx, tag in enumerate(tags)
        ]

    # Hash PII
    hash_pii_fields(node, CUSTOMER_PII_PATHS)

    # Audit field
    node['_loaded_at'] = datetime.now(timezone.utc).isoformat()

    return node


def fetch_customers(graphql_client, start_date, end_date, chunk_days=7, page_size=100, max_rows=0):
    """Fetch customers from Shopify GraphQL API with date chunking and pagination."""
    current_start = pendulum.instance(start_date) if not isinstance(start_date, pendulum.DateTime) else start_date
    end_dt = pendulum.instance(end_date) if not isinstance(end_date, pendulum.DateTime) else end_date
    row_count = 0

    while current_start < end_dt:
        chunk_end = min(current_start.add(days=chunk_days), end_dt)
        query_filter = f"updated_at:>='{current_start.to_iso8601_string()}' AND updated_at:<='{chunk_end.to_iso8601_string()}'"

        logger.info(f"Fetching customers: {current_start.to_date_string()} to {chunk_end.to_date_string()}")

        cursor = None
        has_next = True
        page_count = 0

        while has_next:
            variables = {"query": query_filter, "first": page_size}
            if cursor:
                variables["after"] = cursor

            response = graphql_client(CUSTOMERS_QUERY, variables)

            if not response or 'data' not in response:
                logger.error("No data in GraphQL response")
                break

            customers_data = response['data'].get('customers', {})
            edges = customers_data.get('edges', [])
            page_info = customers_data.get('pageInfo', {})

            page_count += 1
            logger.info(f"  Page {page_count}: {len(edges)} customers")

            for edge in edges:
                node = edge.get('node', {})
                yield transform_customer(node)
                row_count += 1
                if max_rows and row_count >= max_rows:
                    logger.info(f"Reached max_rows limit ({max_rows}), stopping")
                    return

            has_next = page_info.get('hasNextPage', False)
            cursor = page_info.get('endCursor')

        current_start = chunk_end


@dlt.resource(
    name="customers",
    primary_key=['shopId', 'id'],
    write_disposition="merge",
    table_name="customer",
    **CUSTOMER_HINTS,
)
def customers_table(
    graphql_client,
    updated_at=dlt.sources.incremental("updatedAt", initial_value="2020-01-01T00:00:00Z"),
    chunk_days=7,
    start_date_override=None,
    end_date_override=None,
    backfill_days=0,
    page_size=100,
    max_rows=0,
):
    """DLT resource for Shopify customers with incremental loading."""
    start_date = get_start_date_with_backfill(
        updated_at,
        backfill_days,
        start_date_override=start_date_override,
    )
    end_date = get_end_date(end_date_override)

    logger.info(f"Customers: {start_date} to {end_date} (backfill={backfill_days}d)")

    yield from fetch_customers(graphql_client, start_date, end_date, chunk_days, page_size, max_rows)

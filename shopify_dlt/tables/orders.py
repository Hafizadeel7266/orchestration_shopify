"""Order table resource -- yields raw GraphQL nodes for DLT auto-flatten."""
import logging
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Sequence

import dlt
import pendulum

from ..config import (
    SHOPIFY_BULK_POLL_INTERVAL_SECONDS,
    SHOPIFY_ORDERS_BULK_ID_SNAPSHOT_ENABLED,
    SHOPIFY_ORDERS_HYDRATE_BATCH_SIZE,
    SHOPIFY_ORDERS_SNAPSHOT_LOOKBACK_DAYS,
)
from ..helpers.bulk_utils import BulkOperationSnapshot, get_bulk_order_snapshots
from ..helpers.dates import get_end_date, get_start_date_with_backfill
from ..helpers.transforms import (
    parse_gids, hash_pii_fields,
    ORDER_PII_PATHS, REFUND_PII_PATHS, JOURNEY_PII_PATHS,
)
from ..queries.orders import ORDERS_BY_IDS_QUERY, ORDERS_QUERY
from ..schemas.schemas import ORDER_HINTS

logger = logging.getLogger("shopify_pipeline.orders")

HYDRATE_BATCH_SIZE_LADDER: Sequence[int] = (25, 10, 5)
SOURCE_STATE_KEY = "shopify_orders_bulk_hydrate"


def _unwrap_edges(node, field):
    """Replace {field: {nodes: [...]}} or {field: {edges: [{node: ...}]}} with flat list."""
    container = node.get(field)
    if isinstance(container, dict):
        if 'nodes' in container:
            node[field] = container['nodes']
        elif 'edges' in container:
            node[field] = [e['node'] for e in container['edges']]


def transform_order(node):
    """Apply minimal transforms to raw GraphQL order node."""
    # Unwrap edge/node wrappers into flat lists
    for field in ['lineItems', 'refunds',
                   'discountApplications', 'shippingLines']:
        _unwrap_edges(node, field)

    # Unwrap nested edges inside refunds
    for refund in node.get('refunds', []):
        _unwrap_edges(refund, 'refundLineItems')
        _unwrap_edges(refund, 'orderAdjustments')

    # Unwrap line item edges
    for li in node.get('lineItems', []):
        _unwrap_edges(li, 'taxLines')
        _unwrap_edges(li, 'discountAllocations')

    # Warn if line items were truncated (>250)
    li_container = node.get('lineItems')
    if isinstance(li_container, dict):
        li_page = li_container.get('pageInfo', {})
        if li_page.get('hasNextPage'):
            logger.warning(f"Order {node.get('id')} has >250 line items — data truncated")

    # Normalize discount application polymorphic value field + add index
    for idx, da in enumerate(node.get('discountApplications', [])):
        da['index'] = idx
        value = da.pop('value', None)
        if isinstance(value, dict):
            if 'percentage' in value:
                da['valueType'] = 'percentage'
                da['valuePercentage'] = value['percentage']
            elif 'amount' in value:
                da['valueType'] = 'fixed_amount'
                da['valueAmount'] = float(value['amount']) if value.get('amount') else None
                da['valueCurrencyCode'] = value.get('currencyCode')

    # Flatten customer amountSpent to top level
    customer = node.get('customer')
    if isinstance(customer, dict):
        amt_spent = customer.pop('amountSpent', None)
        if isinstance(amt_spent, dict):
            customer['totalSpent'] = amt_spent.get('amount')
            customer['totalSpentCurrency'] = amt_spent.get('currencyCode')

    # Note: customerJourneySummary.firstVisit.source and .sourceType are already
    # scalar strings from GraphQL — no flattening needed. DLT will flatten them
    # as customerJourneySummary__firstVisit__source etc.

    # Parse all GIDs recursively
    parse_gids(node)

    # --- Inject parent FKs and positional indexes into child/grandchild rows ---
    # Runs after parse_gids so all id values are already integers
    order_id = node.get('id')

    for li in node.get('lineItems', []):
        li['orderId'] = order_id
        for tax_idx, tl in enumerate(li.get('taxLines', [])):
            tl['lineItemId'] = li.get('id')
            tl['index'] = tax_idx
        for da_idx, da in enumerate(li.get('discountAllocations', [])):
            da['lineItemId'] = li.get('id')
            da['index'] = da_idx
        for ca_idx, ca in enumerate(li.get('customAttributes', [])):
            ca['lineItemId'] = li.get('id')
            ca['index'] = ca_idx

    for refund in node.get('refunds', []):
        refund['orderId'] = order_id
        for rli in refund.get('refundLineItems', []):
            rli['refundId'] = refund.get('id')
        for oa in refund.get('orderAdjustments', []):
            oa['refundId'] = refund.get('id')

    for da in node.get('discountApplications', []):
        da['orderId'] = order_id

    for idx, tl in enumerate(node.get('taxLines', [])):
        tl['orderId'] = order_id
        tl['index'] = idx

    for sl in node.get('shippingLines', []):
        sl['orderId'] = order_id
        for stl_idx, stl in enumerate(sl.get('taxLines', [])):
            stl['shippingLineId'] = sl.get('id')
            stl['index'] = stl_idx

    # Hash PII fields
    hash_pii_fields(node, ORDER_PII_PATHS)
    for refund in node.get('refunds', []):
        hash_pii_fields(refund, REFUND_PII_PATHS)
    journey = node.get('customerJourneySummary')
    if isinstance(journey, dict):
        for visit_key in ['firstVisit', 'lastVisit']:
            visit = journey.get(visit_key)
            if isinstance(visit, dict):
                hash_pii_fields(visit, JOURNEY_PII_PATHS)

    # Add audit field
    node['_loaded_at'] = datetime.now(timezone.utc).isoformat()

    return node


def _iter_hydrate_batch_sizes(initial_batch_size: int) -> Iterable[int]:
    """Yield decreasing hydrate batch sizes starting at the configured cap."""
    yielded = set()
    for candidate in HYDRATE_BATCH_SIZE_LADDER:
        size = min(initial_batch_size, candidate)
        if size not in yielded:
            yielded.add(size)
            yield size


def _next_hydrate_batch_size(current_batch_size: int, initial_batch_size: int) -> int:
    """Return the next lower hydrate batch size, or the current size at the floor."""
    ladder = list(_iter_hydrate_batch_sizes(initial_batch_size))
    for candidate in ladder:
        if candidate < current_batch_size:
            return candidate
    return current_batch_size


def _compute_snapshot_start_date(start_date, end_date, snapshot_lookback_days: int):
    """Use the older of incremental/backfill start and the snapshot overlap window."""
    current_start = pendulum.instance(start_date) if not isinstance(start_date, pendulum.DateTime) else start_date
    end_dt = pendulum.instance(end_date) if not isinstance(end_date, pendulum.DateTime) else end_date
    if snapshot_lookback_days <= 0:
        return current_start

    overlap_start = end_dt.subtract(days=snapshot_lookback_days)
    return min(current_start, overlap_start)


def _get_orders_source_state() -> Dict[str, Dict[str, Dict[str, str]]]:
    """Read or initialize the source-state manifest for hydrated orders."""
    state = dlt.current.source_state().setdefault(SOURCE_STATE_KEY, {})
    state.setdefault("hydrated_orders_by_shop", {})
    return state


def _get_shop_hydrated_manifest(
    state: Dict[str, Dict[str, Dict[str, str]]],
    shop_id,
) -> Dict[str, str]:
    """Return the hydrate manifest for a single shop."""
    shop_key = str(shop_id)
    manifests = state.setdefault("hydrated_orders_by_shop", {})
    manifests.setdefault(shop_key, {})
    return manifests[shop_key]


def _prune_hydrated_manifest(manifest: Dict[str, str], min_updated_at: str) -> None:
    """Drop hydrated ids that are older than the active overlap window."""
    stale_ids = [gid for gid, updated_at in manifest.items() if updated_at < min_updated_at]
    for gid in stale_ids:
        manifest.pop(gid, None)


def _select_snapshots_to_hydrate(
    snapshots: Sequence[BulkOperationSnapshot],
    hydrated_manifest: Dict[str, str],
) -> List[BulkOperationSnapshot]:
    """Select snapshots that are new or whose updatedAt changed since last hydrate."""
    candidates: List[BulkOperationSnapshot] = []
    for snapshot in snapshots:
        if hydrated_manifest.get(snapshot.gid) != snapshot.updated_at:
            candidates.append(snapshot)
    return candidates


def _build_snapshot_query_filter(start_date, end_date) -> str:
    return (
        f"updated_at:>='{start_date.to_iso8601_string()}' "
        f"AND updated_at:<='{end_date.to_iso8601_string()}'"
    )


def _hydrate_orders_by_ids(graphql_client, order_gids: Sequence[str], batch_size: int = 25) -> List[dict]:
    """Hydrate full order payloads by gid using a run-wide batch-size ladder."""
    hydrated_orders: List[dict] = []
    current_batch_size = next(_iter_hydrate_batch_sizes(batch_size))
    index = 0

    while index < len(order_gids):
        effective_batch_size = min(current_batch_size, len(order_gids) - index)
        chunk = list(order_gids[index:index + effective_batch_size])
        try:
            response = graphql_client(ORDERS_BY_IDS_QUERY, {"ids": chunk})
            nodes = response.get("data", {}).get("nodes", [])
            if len(nodes) != len(chunk):
                raise ValueError(
                    f"Hydrate response size mismatch. expected={len(chunk)} actual={len(nodes)}"
                )
            for gid, node in zip(chunk, nodes):
                if node is None:
                    raise ValueError(f"Hydrate returned null node for order {gid}")
                hydrated_orders.append(node)
            index += effective_batch_size
        except Exception as exc:
            next_batch_size = _next_hydrate_batch_size(current_batch_size, batch_size)
            if next_batch_size == current_batch_size:
                raise
            logger.warning(
                "Reducing hydrate batch size from %s to %s after failure: %s",
                current_batch_size,
                next_batch_size,
                exc,
            )
            current_batch_size = next_batch_size

    return hydrated_orders


def fetch_orders_graphql(graphql_client, start_date, end_date, chunk_days=7, page_size=100, max_rows=0):
    """Legacy orders fetch via GraphQL connection pagination."""
    current_start = pendulum.instance(start_date) if not isinstance(start_date, pendulum.DateTime) else start_date
    end_dt = pendulum.instance(end_date) if not isinstance(end_date, pendulum.DateTime) else end_date
    row_count = 0

    while current_start < end_dt:
        chunk_end = min(current_start.add(days=chunk_days), end_dt)
        query_filter = _build_snapshot_query_filter(current_start, chunk_end)

        logger.info("Fetching orders via legacy pagination: %s", query_filter)

        cursor = None
        has_next = True
        page_count = 0

        while has_next:
            variables = {"query": query_filter, "first": page_size}
            if cursor:
                variables["after"] = cursor

            response = graphql_client(ORDERS_QUERY, variables)

            if not response or 'data' not in response:
                logger.error("No data in GraphQL response")
                break

            orders_data = response['data'].get('orders', {})
            edges = orders_data.get('edges', [])
            page_info = orders_data.get('pageInfo', {})

            page_count += 1
            logger.info(f"  Legacy page {page_count}: {len(edges)} orders")

            for edge in edges:
                node = edge.get('node', {})
                yield transform_order(node)
                row_count += 1
                if max_rows and row_count >= max_rows:
                    logger.info(f"Reached max_rows limit ({max_rows}), stopping")
                    return

            has_next = page_info.get('hasNextPage', False)
            cursor = page_info.get('endCursor')

        current_start = chunk_end


def fetch_orders_bulk(
    graphql_client,
    start_date,
    end_date,
    shop_id,
    hydrate_batch_size=25,
    max_rows=0,
    poll_interval_seconds=5,
):
    """Fetch orders via bulk id snapshot followed by GraphQL hydration."""
    query_filter = _build_snapshot_query_filter(start_date, end_date)
    logger.info("Snapshotting orders via bulk query: %s", query_filter)

    snapshots = get_bulk_order_snapshots(
        graphql_client,
        query_filter,
        poll_interval_seconds=poll_interval_seconds,
    )

    state = _get_orders_source_state()
    hydrated_manifest = _get_shop_hydrated_manifest(state, shop_id)
    _prune_hydrated_manifest(hydrated_manifest, start_date.to_iso8601_string())

    snapshots_to_hydrate = _select_snapshots_to_hydrate(snapshots, hydrated_manifest)
    logger.info(
        "Bulk snapshot discovered %s orders; %s require hydration",
        len(snapshots),
        len(snapshots_to_hydrate),
    )

    if max_rows:
        snapshots_to_hydrate = snapshots_to_hydrate[:max_rows]

    order_gids = [snapshot.gid for snapshot in snapshots_to_hydrate]
    hydrated_orders = _hydrate_orders_by_ids(
        graphql_client,
        order_gids,
        batch_size=hydrate_batch_size,
    )

    hydrated_by_gid = {}
    for order in hydrated_orders:
        gid = order.get("id")
        if not gid:
            raise ValueError("Hydrated order missing id")
        hydrated_by_gid[gid] = order

    missing_gids = [gid for gid in order_gids if gid not in hydrated_by_gid]
    if missing_gids:
        raise ValueError(f"Failed to hydrate {len(missing_gids)} orders from bulk snapshot")

    for snapshot in snapshots_to_hydrate:
        hydrated_manifest[snapshot.gid] = snapshot.updated_at

    for snapshot in snapshots_to_hydrate:
        yield transform_order(hydrated_by_gid[snapshot.gid])


@dlt.resource(
    name="orders",
    primary_key=['shopId', 'id'],
    write_disposition="merge",
    table_name="order",
    **ORDER_HINTS,
)
def orders_table(
    graphql_client,
    shop_id,
    updated_at=dlt.sources.incremental("updatedAt", initial_value="2020-01-01T00:00:00Z"),
    chunk_days=7,
    start_date_override=None,
    end_date_override=None,
    backfill_days=0,
    page_size=100,
    max_rows=0,
    hydrate_batch_size=SHOPIFY_ORDERS_HYDRATE_BATCH_SIZE,
    snapshot_lookback_days=SHOPIFY_ORDERS_SNAPSHOT_LOOKBACK_DAYS,
    bulk_id_snapshot_enabled=SHOPIFY_ORDERS_BULK_ID_SNAPSHOT_ENABLED,
    bulk_poll_interval_seconds=SHOPIFY_BULK_POLL_INTERVAL_SECONDS,
):
    """DLT resource for Shopify orders with incremental loading."""
    start_date = get_start_date_with_backfill(
        updated_at,
        backfill_days,
        start_date_override=start_date_override,
    )
    end_date = get_end_date(end_date_override)
    snapshot_start_date = _compute_snapshot_start_date(start_date, end_date, snapshot_lookback_days)

    logger.info(
        "Orders: start=%s snapshot_start=%s end=%s backfill=%sd bulk_ids=%s",
        start_date,
        snapshot_start_date,
        end_date,
        backfill_days,
        bulk_id_snapshot_enabled,
    )

    if bulk_id_snapshot_enabled:
        yield from fetch_orders_bulk(
            graphql_client,
            snapshot_start_date,
            end_date,
            shop_id=shop_id,
            hydrate_batch_size=hydrate_batch_size,
            max_rows=max_rows,
            poll_interval_seconds=bulk_poll_interval_seconds,
        )
        return

    yield from fetch_orders_graphql(graphql_client, start_date, end_date, chunk_days, page_size, max_rows)

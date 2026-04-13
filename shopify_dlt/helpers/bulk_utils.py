"""Helpers for Shopify GraphQL bulk operations."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Optional

import requests

from .exceptions import GraphQLError

logger = logging.getLogger("shopify_pipeline.bulk_utils")


BULK_OPERATION_RUN_QUERY = """
mutation RunBulkQuery($query: String!) {
  bulkOperationRunQuery(query: $query) {
    bulkOperation {
      id
      status
    }
    userErrors {
      field
      message
    }
  }
}
"""


CURRENT_BULK_OPERATION_QUERY = """
query CurrentBulkOperation {
  currentBulkOperation(type: QUERY) {
    id
    status
    errorCode
    createdAt
    completedAt
    objectCount
    fileSize
    url
    partialDataUrl
  }
}
"""


TERMINAL_BULK_STATUSES = {"COMPLETED", "FAILED", "CANCELED", "CANCELING", "EXPIRED"}


@dataclass(frozen=True)
class BulkOperationSnapshot:
    """Lightweight order snapshot row returned from a bulk JSONL export."""

    gid: str
    updated_at: str
    created_at: Optional[str] = None


def escape_graphql_string(value: str) -> str:
    """Escape a string for embedding inside a GraphQL multi-line string."""
    return value.replace("\\", "\\\\").replace('"', '\\"')


def build_orders_bulk_id_query(query_filter: str) -> str:
    """Build a lightweight bulk query that snapshots order ids in a window."""
    escaped_filter = escape_graphql_string(query_filter)
    return f"""
{{
  orders(query: "{escaped_filter}", sortKey: UPDATED_AT) {{
    edges {{
      node {{
        id
        updatedAt
        createdAt
      }}
    }}
  }}
}}
""".strip()


def run_bulk_query(
    graphql_client: Callable[[str, Optional[Dict]], Dict],
    bulk_query: str,
    poll_interval_seconds: int = 5,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> Dict:
    """Start a Shopify bulk query and poll until it reaches a terminal state."""
    response = graphql_client(BULK_OPERATION_RUN_QUERY, {"query": bulk_query})
    run_result = response.get("data", {}).get("bulkOperationRunQuery", {})
    user_errors = run_result.get("userErrors", [])
    if user_errors:
        message = "; ".join(err.get("message", "Unknown user error") for err in user_errors)
        raise GraphQLError(f"Bulk query rejected: {message}")

    operation = run_result.get("bulkOperation")
    if not operation or not operation.get("id"):
        raise GraphQLError("Bulk query did not return an operation id")

    operation_id = operation["id"]
    logger.info("Started Shopify bulk query %s", operation_id)

    while True:
        status_response = graphql_client(CURRENT_BULK_OPERATION_QUERY)
        current = status_response.get("data", {}).get("currentBulkOperation")
        if not current:
            raise GraphQLError("No current bulk operation returned while polling")
        if current.get("id") != operation_id:
            raise GraphQLError(
                f"Unexpected bulk operation while polling. Expected {operation_id}, got {current.get('id')}"
            )

        status = current.get("status")
        logger.info(
            "Bulk query %s status=%s objectCount=%s",
            operation_id,
            status,
            current.get("objectCount"),
        )
        if status in TERMINAL_BULK_STATUSES:
            return current

        sleep_fn(poll_interval_seconds)


def download_bulk_jsonl(
    url: str,
    timeout: int = 300,
    get_fn: Callable[..., requests.Response] = requests.get,
) -> str:
    """Download a completed bulk JSONL file."""
    response = get_fn(url, timeout=timeout)
    response.raise_for_status()
    return response.text


def parse_bulk_order_id_jsonl(payload: str) -> List[BulkOperationSnapshot]:
    """Parse and dedupe lightweight bulk order snapshot rows by gid."""
    snapshots: Dict[str, BulkOperationSnapshot] = {}
    for raw_line in payload.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        row = json.loads(line)
        gid = row.get("id")
        updated_at = row.get("updatedAt")
        if not gid or not updated_at:
            continue
        snapshot = BulkOperationSnapshot(
            gid=gid,
            updated_at=updated_at,
            created_at=row.get("createdAt"),
        )
        existing = snapshots.get(gid)
        if existing is None or snapshot.updated_at > existing.updated_at:
            snapshots[gid] = snapshot

    return sorted(snapshots.values(), key=lambda item: (item.updated_at, item.gid))


def ensure_bulk_operation_completed(operation: Dict) -> str:
    """Return the completed bulk url or raise on an incomplete/failed operation."""
    status = operation.get("status")
    if status != "COMPLETED":
        error_code = operation.get("errorCode")
        raise GraphQLError(
            f"Bulk operation finished with status={status} errorCode={error_code}"
        )

    url = operation.get("url") or operation.get("partialDataUrl")
    if not url:
        raise GraphQLError("Bulk operation completed without a result URL")
    return url


def get_bulk_order_snapshots(
    graphql_client: Callable[[str, Optional[Dict]], Dict],
    query_filter: str,
    poll_interval_seconds: int = 5,
    bulk_timeout: int = 300,
    sleep_fn: Callable[[float], None] = time.sleep,
    get_fn: Callable[..., requests.Response] = requests.get,
) -> List[BulkOperationSnapshot]:
    """Execute a bulk order snapshot query and return deduped order ids."""
    bulk_query = build_orders_bulk_id_query(query_filter)
    operation = run_bulk_query(
        graphql_client,
        bulk_query,
        poll_interval_seconds=poll_interval_seconds,
        sleep_fn=sleep_fn,
    )
    url = ensure_bulk_operation_completed(operation)
    payload = download_bulk_jsonl(url, timeout=bulk_timeout, get_fn=get_fn)
    snapshots = parse_bulk_order_id_jsonl(payload)
    logger.info("Bulk snapshot returned %s distinct orders", len(snapshots))
    return snapshots

"""GraphQL client with retry logic and cost-based throttling."""

import requests
import time
from typing import Dict, Optional, Sequence
from .exceptions import RateLimitError, AuthenticationError, GraphQLError
from .logging_config import get_logger

try:
    from ..config.settings import MAX_RETRIES, REQUEST_TIMEOUT
except ImportError:
    from config.settings import MAX_RETRIES, REQUEST_TIMEOUT

logger = get_logger("request_utils")

PLUS_PAGE_SIZE_LADDER: Sequence[int] = (250, 100, 50, 40, 30, 20, 10)
NON_PLUS_PAGE_SIZE_LADDER: Sequence[int] = (100, 50, 40, 30, 20, 10)
PAGE_SIZE_BACKOFF_SLEEP_THRESHOLD = 5.0


class AdaptiveGraphQLClient:
    """GraphQL client with adaptive page-size backoff."""

    def __init__(self, graphql_url: str, access_token: str):
        self.graphql_url = graphql_url
        self.access_token = access_token
        self.page_size_ladder = NON_PLUS_PAGE_SIZE_LADDER
        self.page_size_cap = NON_PLUS_PAGE_SIZE_LADDER[0]
        self.current_page_size = NON_PLUS_PAGE_SIZE_LADDER[0]
        self.plan_is_plus = False

    def configure_rate_profile(self, plan_is_plus: Optional[bool], page_size_cap: int) -> None:
        """Configure the adaptive page-size ladder after the bootstrap query."""
        self.plan_is_plus = bool(plan_is_plus)
        self.page_size_ladder = PLUS_PAGE_SIZE_LADDER if self.plan_is_plus else NON_PLUS_PAGE_SIZE_LADDER
        self.page_size_cap = page_size_cap
        self.current_page_size = min(self.page_size_ladder[0], self.page_size_cap)
        logger.info(
            "Configured Shopify rate profile: plus=%s, cap=%s, initial_page_size=%s",
            self.plan_is_plus,
            self.page_size_cap,
            self.current_page_size,
        )

    def _apply_page_size(self, variables: Optional[Dict]) -> Dict:
        request_variables = dict(variables or {})
        first = request_variables.get("first")
        if isinstance(first, int):
            request_variables["first"] = min(first, self.page_size_cap, self.current_page_size)
        return request_variables

    def decrease_page_size(self, reason: str) -> None:
        """Move down the page-size ladder once for the remainder of the run."""
        old_page_size = self.current_page_size
        for candidate in self.page_size_ladder:
            if candidate < old_page_size and candidate <= self.page_size_cap:
                self.current_page_size = candidate
                logger.warning(
                    "Reducing Shopify page size from %s to %s due to %s",
                    old_page_size,
                    self.current_page_size,
                    reason,
                )
                return

        logger.info(
            "Shopify page size already at floor (%s); keeping current size after %s",
            self.current_page_size,
            reason,
        )

    def can_decrease_page_size(self) -> bool:
        """Return True when the current page size can still be reduced."""
        return any(
            candidate < self.current_page_size and candidate <= self.page_size_cap
            for candidate in self.page_size_ladder
        )

    def __call__(self, query: str, variables: Dict = None) -> Dict:
        headers = {
            "X-Shopify-Access-Token": self.access_token,
            "Content-Type": "application/json",
        }
        last_exception = None

        for attempt in range(MAX_RETRIES):
            try:
                payload = {"query": query, "variables": self._apply_page_size(variables)}

                response = requests.post(
                    self.graphql_url,
                    headers=headers,
                    json=payload,
                    timeout=REQUEST_TIMEOUT,
                )

                if response.status_code == 401:
                    raise AuthenticationError("Invalid or expired token")

                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 2 ** attempt))
                    self.decrease_page_size("http_429")
                    logger.warning(f"Rate limited (429). Retrying after {retry_after}s")
                    time.sleep(retry_after)
                    last_exception = RateLimitError(f"Rate limited after {MAX_RETRIES} attempts")
                    continue

                response.raise_for_status()
                result = response.json()

                if 'errors' in result:
                    errors = result['errors']

                    throttled = _is_throttled(errors)
                    if throttled:
                        sleep_time = 2 ** attempt
                        self.decrease_page_size("graphql_throttled")
                        logger.warning(f"GraphQL THROTTLED. Retrying in {sleep_time}s")
                        last_exception = GraphQLError("THROTTLED")
                        time.sleep(sleep_time)
                        continue

                    if 'data' in result and result['data'] is not None:
                        for err in errors:
                            logger.warning(f"GraphQL warning (non-fatal): {err.get('message', err)}")
                    else:
                        error_msg = errors[0].get('message', str(errors[0]))
                        if _is_query_cost_limit_error(error_msg) and self.can_decrease_page_size():
                            self.decrease_page_size("single_query_cost_limit")
                            logger.warning(
                                "GraphQL query exceeded single-query cost limit. Retrying with page size %s",
                                self.current_page_size,
                            )
                            last_exception = GraphQLError(error_msg)
                            continue
                        logger.error(f"GraphQL Error: {error_msg}")
                        raise GraphQLError(error_msg)

                sleep_time = _apply_cost_throttle(result)
                if sleep_time >= PAGE_SIZE_BACKOFF_SLEEP_THRESHOLD:
                    self.decrease_page_size(f"cost_throttle_sleep_{sleep_time:.1f}s")
                return result

            except AuthenticationError:
                raise
            except GraphQLError:
                raise
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed: {e}")
                last_exception = e
                if attempt == MAX_RETRIES - 1:
                    logger.error(f"Max retries exceeded: {e}")
                    raise
                sleep_time = 2 ** attempt
                logger.info(f"Retrying in {sleep_time}s")
                time.sleep(sleep_time)

        raise RateLimitError(
            f"Max retries ({MAX_RETRIES}) exhausted. Last error: {last_exception}"
        )


def create_graphql_client(shop_url: str, access_token: str):
    """Create a GraphQL client function with retry logic."""
    GRAPHQL_URL = f"https://{shop_url}/admin/api/2026-01/graphql.json"

    logger.info(f"Creating GraphQL client for shop: {shop_url}")
    return AdaptiveGraphQLClient(GRAPHQL_URL, access_token)


def _is_throttled(errors: list) -> bool:
    """Check if GraphQL errors indicate a THROTTLED response."""
    for err in errors:
        # Check extensions.code
        extensions = err.get('extensions', {})
        if extensions.get('code') == 'THROTTLED':
            return True
        # Check message text as fallback
        msg = err.get('message', '')
        if 'THROTTLED' in msg.upper() or 'Throttled' in msg:
            return True
    return False


def _is_query_cost_limit_error(message: str) -> bool:
    """Check if GraphQL error indicates the query cost exceeds the max limit."""
    message_upper = message.upper()
    return "QUERY COST IS" in message_upper and "EXCEEDS THE SINGLE QUERY MAX COST LIMIT" in message_upper


def _apply_cost_throttle(result: Dict) -> float:
    """Sleep if the query cost bucket is running low.

    Shopify GraphQL returns ``extensions.cost.throttleStatus`` with:
      - currentlyAvailable: points remaining
      - restoreRate: points restored per second

    And ``extensions.cost.requestedQueryCost`` for the cost of the query.
    If available points < requested cost, we sleep just long enough for the
    bucket to refill.
    """
    extensions = result.get('extensions', {})
    cost_info = extensions.get('cost', {})
    if not cost_info:
        return 0.0

    throttle_status = cost_info.get('throttleStatus', {})
    currently_available = throttle_status.get('currentlyAvailable')
    restore_rate = throttle_status.get('restoreRate')
    requested_cost = cost_info.get('requestedQueryCost')

    if currently_available is None or restore_rate is None or requested_cost is None:
        return 0.0

    if restore_rate <= 0:
        return 0.0

    if currently_available < requested_cost:
        deficit = requested_cost - currently_available
        sleep_time = deficit / restore_rate
        logger.info(
            f"Throttle bucket low: {currently_available}/{requested_cost} points available. "
            f"Sleeping {sleep_time:.1f}s (restore rate: {restore_rate}/s)"
        )
        time.sleep(sleep_time)
        return sleep_time

    return 0.0

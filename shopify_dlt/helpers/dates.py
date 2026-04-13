"""Date handling with backfill support."""

import dlt
import logging
from typing import Optional
from dlt.common import pendulum

logger = logging.getLogger("shopify_pipeline.dates")


def coerce_datetime(value) -> Optional[pendulum.DateTime]:
    """Convert a string or datetime-like value to a UTC pendulum datetime."""
    if value is None:
        return None
    if hasattr(value, "in_timezone"):
        return value.in_timezone("UTC")
    return pendulum.parse(str(value)).in_timezone("UTC")


def get_start_date_with_backfill(
    incremental: dlt.sources.incremental[str],
    backfill_days: int = 0,
    start_date_override=None,
) -> pendulum.DateTime:
    """
    Get start date with backfill support.

    Args:
        incremental: dlt incremental cursor
        backfill_days: Number of days to backfill (0 = normal incremental)
        start_date_override: Explicit start datetime override

    Returns:
        Start date as pendulum DateTime
    """
    if start_date_override is not None:
        start_date = coerce_datetime(start_date_override)
        logger.info("FIXED WINDOW MODE: start=%s", start_date)
        return start_date

    if backfill_days > 0:
        start_date = pendulum.now("UTC").subtract(days=backfill_days).start_of("day")
        logger.info(
            "BACKFILL MODE: Loading last %s full UTC day(s) starting at %s",
            backfill_days,
            start_date,
        )
        return start_date

    cursor_value = incremental.start_value
    if not cursor_value:
        raise ValueError("No cursor value available from incremental")

    start_date = pendulum.parse(cursor_value)
    logger.info(f"Incremental mode: start={start_date.date()}")
    return start_date


def get_end_date(end_date_override=None) -> pendulum.DateTime:
    """Resolve an explicit end datetime override or default to now."""
    if end_date_override is not None:
        end_date = coerce_datetime(end_date_override)
        logger.info("FIXED WINDOW MODE: end=%s", end_date)
        return end_date
    return pendulum.now("UTC")


def validate_backfill_days(backfill_days: int, max_days: int = 90) -> int:
    """Validate backfill days against limits."""
    if backfill_days < 0:
        logger.warning("Negative backfill days not allowed. Using 0.")
        return 0
    if backfill_days > max_days:
        logger.warning(f"Backfill days {backfill_days} exceeds maximum {max_days}. Capping to {max_days}.")
        return max_days
    return backfill_days

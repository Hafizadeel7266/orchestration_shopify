"""Dagster schedules for Shopify pipeline assets.

Schedule frequency based on data freshness requirements:
- Orders:      every 30 minutes (revenue-critical)
- Customers:   every 6 hours (moderate change rate)
- Catalog:     every 12 hours (products + collections)
- Shop:        daily (rarely changes)
- Full sync:   weekly safety net (disabled by default)
"""

from dagster import ScheduleDefinition, DefaultScheduleStatus

from ..jobs import (
  orders_job,
  customers_job,
  catalog_job,
  shop_job,
  full_sync_job,
)
from ..utils.constants import (
  ORDERS_CRON,
  CUSTOMERS_CRON,
  CATALOG_CRON,
  SHOP_CRON,
  FULL_SYNC_CRON,
)

orders_schedule = ScheduleDefinition(
  name="shopify_orders_every_30min",
  job=orders_job,
  cron_schedule=ORDERS_CRON,
  default_status=DefaultScheduleStatus.RUNNING,
  description="Sync orders every 30 minutes",
  execution_timezone="UTC",
)

customers_schedule = ScheduleDefinition(
  name="shopify_customers_every_6h",
  job=customers_job,
  cron_schedule=CUSTOMERS_CRON,
  default_status=DefaultScheduleStatus.RUNNING,
  description="Sync customers every 6 hours",
  execution_timezone="UTC",
)

catalog_schedule = ScheduleDefinition(
  name="shopify_catalog_every_12h",
  job=catalog_job,
  cron_schedule=CATALOG_CRON,
  default_status=DefaultScheduleStatus.RUNNING,
  description="Sync products + collections every 12 hours",
  execution_timezone="UTC",
)

shop_schedule = ScheduleDefinition(
  name="shopify_shop_daily",
  job=shop_job,
  cron_schedule=SHOP_CRON,
  default_status=DefaultScheduleStatus.RUNNING,
  description="Refresh shop metadata daily at 3 AM UTC",
  execution_timezone="UTC",
)

full_sync_schedule = ScheduleDefinition(
  name="shopify_full_sync_weekly",
  job=full_sync_job,
  cron_schedule=FULL_SYNC_CRON,
  default_status=DefaultScheduleStatus.STOPPED,
  description="Weekly full sync safety net (disabled by default)",
  execution_timezone="UTC",
)

all_schedules = [
  orders_schedule,
  customers_schedule,
  catalog_schedule,
  shop_schedule,
  full_sync_schedule,
]
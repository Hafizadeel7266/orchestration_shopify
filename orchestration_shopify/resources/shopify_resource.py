"""Dagster resource that wraps the existing shopify_dlt pipeline runner."""

import logging
from typing import Optional, Sequence, Tuple, Any

from dagster import ConfigurableResource
from pydantic import Field

logger = logging.getLogger("dagster.shopify")


class ShopifyPipelineResource(ConfigurableResource):
    """Dagster resource wrapping the existing shopify_dlt pipeline.

    This resource provides a clean interface for Dagster assets
    to execute the dlt pipeline without modifying any existing code.
    """

    # ── Shopify Connection ──
    shop_url: str = Field(
        default="",
        description="Shopify store URL (overrides .env if set)",
    )
    access_token: str = Field(
        default="",
        description="Shopify Admin API access token (overrides .env if set)",
    )

    # ── Pipeline Settings ──
    destination: str = Field(
        default="duckdb",
        description="Destination: duckdb, bigquery, or snowflake",
    )
    destination_path: str = Field(
        default="data/duckdb/shopify.duckdb",
        description="DuckDB file path (ignored for cloud destinations)",
    )
    pipeline_name: str = Field(
        default="shopify_dlt",
        description="dlt pipeline name",
    )
    dataset_name: str = Field(
        default="shopify_data",
        description="Target dataset name",
    )

    # ── Loading Settings ──
    initial_load_past_days: int = Field(default=7)
    lookback_days: int = Field(default=1)
    chunk_days: int = Field(default=7)
    page_size: int = Field(default=100)
    max_rows: int = Field(default=0)

    def run_resources(
        self,
        resource_names: Sequence[str],
        backfill_days: int = 0,
        start_date_override: Optional[str] = None,
        end_date_override: Optional[str] = None,
    ) -> Tuple[Any, Any]:
        """Execute the dlt pipeline for specific resources.

        Args:
            resource_names: Which dlt resources to run
            backfill_days: Days to backfill (0 = incremental)
            start_date_override: ISO-8601 start date override
            end_date_override: ISO-8601 end date override

        Returns:
            Tuple of (dlt_pipeline, load_info)
        """
        from shopify_dlt.runner import run_pipeline

        logger.info(
            "Dagster executing dlt pipeline: resources=%s, destination=%s, backfill=%sd",
            resource_names,
            self.destination,
            backfill_days,
        )

        pipeline, load_info = run_pipeline(
            shop_url=self.shop_url or None,
            access_token=self.access_token or None,
            initial_load_past_days=self.initial_load_past_days,
            lookback_days=self.lookback_days,
            chunk_days=self.chunk_days,
            backfill_days=backfill_days,
            page_size=self.page_size,
            max_rows=self.max_rows,
            start_date_override=start_date_override,
            end_date_override=end_date_override,
            destination=self.destination,
            destination_path=self.destination_path,
            pipeline_name=self.pipeline_name,
            dataset_name=self.dataset_name,
            resource_names=resource_names,
        )

        return pipeline, load_info

    def parse_load_info(self, load_info) -> dict:
        """Extract useful metadata from dlt load_info for Dagster UI."""
        info = {
            "destination": self.destination,
            "dataset": self.dataset_name,
            "load_id": str(getattr(load_info, "load_id", "unknown")),
        }
        return info
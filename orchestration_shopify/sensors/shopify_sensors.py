"""Dagster sensors for monitoring Shopify pipeline health."""

import logging
from datetime import datetime, timezone

from dagster import (
    RunFailureSensorContext,
    run_failure_sensor,
)

logger = logging.getLogger("dagster.shopify.sensors")


@run_failure_sensor(
    name="shopify_failure_alert_sensor",
    description="Alerts when any Shopify pipeline run fails",
)
def shopify_failure_alert_sensor(context: RunFailureSensorContext):
    """Send alert when a Shopify pipeline run fails.

    Extend this to integrate with:
    - Slack (dagster-slack)
    - PagerDuty
    - Email
    - Microsoft Teams
    """
    run = context.dagster_run
    error = context.failure_event.message if context.failure_event else "Unknown error"

    alert_message = (
        f"🚨 Shopify Pipeline Failure\n"
        f"  Job:    {run.job_name}\n"
        f"  Run ID: {run.run_id}\n"
        f"  Time:   {datetime.now(timezone.utc).isoformat()}\n"
        f"  Error:  {error[:500]}"
    )

    # === SLACK INTEGRATION (uncomment when ready) ===
    # from dagster_slack import SlackResource
    # slack = SlackResource(token="xoxb-your-token")
    # slack.get_client().chat_postMessage(
    #     channel="#data-alerts",
    #     text=alert_message,
    # )

    context.log.error(alert_message)


all_sensors = [
    shopify_failure_alert_sensor,
]
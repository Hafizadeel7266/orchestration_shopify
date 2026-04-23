"""Dagster Definitions — single entry point."""

from dotenv import load_dotenv
from dagster import Definitions

from .assets import all_assets
from .schedules import all_schedules
from .sensors import all_sensors
from .jobs import all_jobs

# Load environment variables
load_dotenv(override=False)

# Dagster Definitions
defs = Definitions(
    assets=all_assets,
    schedules=all_schedules,
    sensors=all_sensors,
    jobs=all_jobs,
)
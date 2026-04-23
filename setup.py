from setuptools import setup, find_packages

setup(
name="orchestration_shopify",
version="0.1.0",
packages=find_packages(),
install_requires=[
    "dagster>=1.7.0",
    "dagster-webserver>=1.7.0",
    "dagster-cloud>=1.7.0",
    "dagster-dlt>=0.1.0",
    "dlt[duckdb]>=1.0.0",
    "dlt[snowflake]>=1.0.0",
    "pendulum>=3.0.0",
    "requests>=2.31.0",
    "python-dotenv>=1.0.0",
    "pyyaml>=6.0",
],
author="Adeel",
description="Dagster orchestration for Shopify DLT pipeline",
python_requires=">=3.9",
)
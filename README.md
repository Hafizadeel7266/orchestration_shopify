
# Orchestration Shopify

Dagster orchestration layer for the [Shopify DLT data pipeline](https://github.com/Hafizadeel7266/shopify-data-pipeline). Provides scheduling, monitoring, backfill, and asset lineage for Shopify data ingestion into DuckDB or Snowflake.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Asset Graph](#asset-graph)
- [Asset Groups](#asset-groups)
- [Schedules](#schedules)
- [Backfill (Partitioned)](#backfill-partitioned)
- [Project Structure](#project-structure)
- [Setup & Installation](#setup--installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Deployment](#deployment)
- [CI/CD & Branch Deployments](#cicd--branch-deployments)
- [Troubleshooting](#troubleshooting)
- [License](#license)

---

## Overview

| Feature | Detail |
| --- | --- |
| **Assets** | 29 software-defined assets (25 incremental + 4 backfill) |
| **Groups** | 6 asset groups (orders, customers, products, collections, shop, backfill) |
| **Schedules** | 5 schedules (30min → weekly) |
| **Sensors** | 1 failure alert sensor |
| **Jobs** | 5 jobs (per group + full sync) |
| **Destinations** | DuckDB (local) / Snowflake (production) |
| **Backfill** | Daily partitioned assets with calendar date picker |
| **Dynamic Icons** | Warehouse icons switch based on DESTINATION env var |

---

## Architecture

```mermaid
flowchart TB
  subgraph Dagster["Dagster Orchestration"]
      UI["Dagster UI\n+ Schedules\n+ Sensors"]

      subgraph Assets["29 Assets"]
          Orders["shopify_orders (12)"]
          Customers["shopify_customers (2)"]
          Products["shopify_products (7)"]
          Collections["shopify_collections (3)"]
          Shop["shopify_shop (1)"]
          Backfill["shopify_backfill (4)"]
      end
  end

  subgraph Pipeline["shopify_dlt Package"]
      Runner["runner.py\nrun_pipeline()"]
      Source["shopify_sources.py"]
      Tables["tables/*.py"]
  end

  subgraph Destinations["Destinations"]
      DuckDB["DuckDB\n(local dev)"]
      Snowflake["Snowflake\n(production)"]
  end

  UI --> Assets
  Assets --> Runner --> Source --> Tables
  Tables --> DuckDB
  Tables --> Snowflake

Asset Graph
mermaid
flowchart TD
  subgraph shopify_orders
      order --> order__line_items
      order --> order__refunds
      order --> order__discount_applications
      order --> order__tax_lines
      order --> order__shipping_lines
      order__line_items --> order__line_items__tax_lines
      order__line_items --> order__line_items__discount_allocations
      order__line_items --> order__line_items__custom_attributes
      order__refunds --> order__refunds__refund_line_items
      order__refunds --> order__refunds__order_adjustments
      order__shipping_lines --> order__shipping_lines__tax_lines
  end

  subgraph shopify_customers
      customer --> customer__tags
  end

  subgraph shopify_products
      product --> product__variants
      product --> product__images
      product --> product__options
      product --> product__tags
      product__variants --> product__variants__selected_options
      product__options --> product__options__option_values
  end

  subgraph shopify_collections
      collection --> collection__rules
      collection --> collection_product
  end

  subgraph shopify_shop
      shop
  end

  subgraph shopify_backfill
      orders_backfill
      customers_backfill
      products_backfill
      collections_backfill
  end

Asset Groups
Group	Assets	Schedule	Description
shopify_orders	12	Every 30 min	Orders + line items, refunds, discounts, shipping, tax lines
shopify_customers	2	Every 6 hours	Customers + tags (PII hashed)
shopify_products	7	Every 12 hours	Products + variants, images, options, tags
shopify_collections	3	Every 12 hours	Collections + rules + product bridge
shopify_shop	1	Daily 3 AM UTC	Shop metadata (full replace)
shopify_backfill	4	Manual (calendar)	Daily partitioned backfill for historical loads
Schedules
Schedule	Cron	Job	Default
shopify_orders_every_30min	*/30 * * * *	shopify_orders_job	✅ Running
shopify_customers_every_6h	0 */6 * * *	shopify_customers_job	✅ Running
shopify_catalog_every_12h	0 */12 * * *	shopify_catalog_job	✅ Running
shopify_shop_daily	0 3 * * *	shopify_shop_job	✅ Running
shopify_full_sync_weekly	0 2 * * 0	shopify_full_sync_job	⏸️ Stopped
Backfill (Partitioned)

Four daily-partitioned assets allow date-range backfills from the Dagster UI:

Asset	Resource	Partition
orders_backfill	orders	Daily (UTC)
customers_backfill	customers	Daily (UTC)
products_backfill	products	Daily (UTC)
collections_backfill	collections	Daily (UTC)
How to Backfill
text
1. Open Dagster UI → Lineage → shopify_backfill group
2. Click on "orders_backfill"
3. Go to Partitions tab
4. Select date range from calendar
5. Click "Launch backfill"


Each partition = one UTC day loaded via start_date_override / end_date_override.

Project Structure
text
orchestration_shopify/
│
├── shopify_dlt/                      # Copied from Repo 1 (auto-synced via GitHub Action)
│   ├── runner.py
│   ├── shopify_sources.py
│   ├── config/
│   ├── tables/
│   ├── queries/
│   ├── schemas/
│   └── helpers/
│
├── orchestration_shopify/            # Dagster orchestration layer
│   ├── __init__.py
│   ├── definitions.py                # Main Dagster entry point
│   ├── assets/
│   │   ├── __init__.py
│   │   ├── shopify_assets.py         # 25 incremental assets with lineage
│   │   └── shopify_backfill.py       # 4 partitioned backfill assets
│   ├── resources/
│   │   ├── __init__.py
│   │   └── shopify_resource.py       # ShopifyPipelineResource
│   ├── schedules/
│   │   ├── __init__.py
│   │   └── shopify_schedules.py      # 5 cron schedules
│   ├── sensors/
│   │   ├── __init__.py
│   │   └── shopify_sensors.py        # Failure alert sensor
│   ├── jobs/
│   │   ├── __init__.py
│   │   └── shopify_jobs.py           # 5 job definitions
│   └── utils/
│       ├── __init__.py
│       └── constants.py              # Groups, owners, crons, kinds
│
├── configs/
│   ├── local.yaml                    # DuckDB config
│   ├── staging.yaml                  # Snowflake staging
│   └── prod.yaml                     # Snowflake production
│
├── .github/workflows/
│   ├── branch_deploy.yml             # PR → branch deployment
│   └── prod_deploy.yml               # Merge → production deployment
│
├── dagster_cloud.yaml                # Dagster Cloud configuration
├── workspace.yaml                    # Local dev configuration
├── pyproject.toml                    # Dagster module config
├── setup.py
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md

Setup & Installation
Prerequisites
Python 3.9+
shopify-data-pipeline
 repo cloned locally
Install
bash
git clone https://github.com/Hafizadeel7266/orchestration_shopify.git
cd orchestration_shopify

python -m venv venv
source venv/bin/activate        # Linux/Mac
# venv\Scripts\activate         # Windows

pip install -e .

Verify
bash
python -c "from shopify_dlt.runner import run_pipeline; print('shopify_dlt ✅')"
python -c "import dagster; print('dagster ✅')"
python -c "from orchestration_shopify.definitions import defs; print('definitions ✅')"

Configuration

Copy .env.example to .env:

bash
cp .env.example .env

Environment Variables
Variable	Default	Description
SHOPIFY_SHOP_URL	—	Shopify store URL
SHOPIFY_ACCESS_TOKEN	—	Admin API access token
PII_HASH_SECRET	—	HMAC secret for PII hashing
DESTINATION	duckdb	duckdb / snowflake
DUCKDB_PATH	data/duckdb/shopify.duckdb	Local DuckDB file path
SNOWFLAKE_ACCOUNT	—	Snowflake account identifier
SNOWFLAKE_USER	—	Snowflake username
SNOWFLAKE_PASSWORD	—	Snowflake password
SNOWFLAKE_DATABASE	—	Snowflake database
SNOWFLAKE_WAREHOUSE	—	Snowflake warehouse
SNOWFLAKE_ROLE	—	Snowflake role
PIPELINE_NAME	shopify_dlt	dlt pipeline name
DATASET_NAME	shopify_data	Target dataset name
Dynamic Destination Icons

The asset icons automatically switch based on DESTINATION:

DESTINATION	Icons Shown
duckdb	🟡 dlt + 🟠 DuckDB
snowflake	🟡 dlt + ❄️ Snowflake
bigquery	🟡 dlt + 🔵 BigQuery
Usage
Launch Dagster UI (Local Dev)
bash
dagster dev -m orchestration_shopify.definitions


Open: http://localhost:3000

Materialize Assets
text
UI → Lineage → Select group → Materialize all

Trigger Specific Job
text
UI → Jobs → shopify_orders_job → Launch run

Run Backfill
text
UI → shopify_backfill → orders_backfill → Partitions → Select dates → Launch

Deployment
Local Development
bash
dagster dev -m orchestration_shopify.definitions

Dagster Cloud

Configured via dagster_cloud.yaml:

yaml
locations:
- location_name: orchestration_shopify
  code_source:
    python_module:
      module_name: orchestration_shopify.definitions
      attribute: defs
  build:
    directory: .

CI/CD & Branch Deployments
Branch Deployment (PR Preview)
text
Open PR → GitHub Action → Dagster Cloud creates isolated branch deployment
→ Test in separate UI → Merge PR → Branch deployment destroyed

Production Deployment
text
Merge to main → GitHub Action → Dagster Cloud production updated

Auto-Sync from shopify-data-pipeline

When shopify_dlt/ code changes in Repo 1:

text
Push to Repo 1 main → GitHub Action → Creates PR in this repo
→ Review & merge → shopify_dlt/ updated

Repo Relationship
text
Repo 1: shopify-data-pipeline        Repo 2: orchestration_shopify
├── shopify_dlt/ (source of truth)    ├── shopify_dlt/ (synced copy)
└── ELT pipeline code                 └── Dagster orchestration code

Sync: GitHub Action auto-creates PR when Repo 1 shopify_dlt/ changes

Troubleshooting
Issue	Solution
SHOPIFY_ACCESS_TOKEN not set	Create .env file — copy from .env.example
Cannot open file shopify.duckdb	Pipeline auto-creates directory; check DUCKDB_PATH
DagsterImportError: relative import	Use dagster dev -m orchestration_shopify.definitions (not -f)
FreshnessPolicy takes no arguments	Removed in Dagster 1.13+ — we don't use it
Assets show wrong destination icon	Restart Dagster after changing DESTINATION in .env
Backfill partition fails	Check date range is within start_date in DailyPartitionsDefinition
GitHub Action sync not triggering	Push must change files inside shopify_dlt/ on main branch

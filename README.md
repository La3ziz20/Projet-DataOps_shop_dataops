# shop-dataops

End-to-end DataOps platform for a multi-currency online shop. Every day, the platform ingests raw shop orders from CSV files and exchange rates from the Frankfurter API (`EUR -> USD`), applies TP1 cleaning and quarantine rules, validates both sources against YAML data contracts, loads them idempotently into a DuckDB warehouse, builds layered dbt models (`staging` and `marts`) with automated data tests, runs freshness and row-count monitoring checks, and writes alerts on any failure.

## Architecture Diagram

```text
+------------------------+       +----------------------------------+
|   Shop Orders (CSV)    |       |   Frankfurter Exchange Rate API  |
| data/raw/orders_*.csv  |       |   https://api.frankfurter.app    |
+-----------+------------+       +-----------------+----------------+
            |                                      |
            v                                      v
+------------------------+       +----------------------------------+
|  TP1 Cleaning Pipeline |       |       FX Ingestion Module        |
|  pipeline/extract.py   |       |         ingestion/fx.py          |
|  pipeline/validate.py  |       |  (stores payload["date"] for FX) |
|  pipeline/transform.py |       +-----------------+----------------+
+-----------+------------+                         |
            |                                      |
     +------+------+                               |
     |             | (>0 rejected)                 |
     |             v                               |
     |   +-------------------+                     |
     |   | Quarantine CSV    |                     |
     |   | data/rejected/    |                     |
     |   +-------------------+                     |
     |                                             |
     +---------------------+-----------------------+
                           |
                           v
         +-----------------------------------+
         |      YAML Contract Validation     |
         |        quality/contracts.py       |
         |  contracts/orders.yaml            |
         |  contracts/frankfurter.yaml       |
         +-----------------+-----------------+
                           |
                           v
         +-----------------------------------+
         |     DuckDB Warehouse (Upsert)     |
         |        warehouse/store.py         |
         |  raw_orders (PK: order_id)        |
         |  raw_fx_rates (PK: date,base,cur) |
         +-----------------+-----------------+
                           |
                           v
         +-----------------------------------+
         |     dbt Models & Schema Tests     |
         |  stg_orders / stg_fx_rates (view) |
         |  mart_revenue_daily (table)       |
         |  analyses/business_questions.sql  |
         +-----------------+-----------------+
                           |
                           v
         +-----------------------------------+
         |   Monitoring & Alerting (Prefect) |
         |  monitoring/checks.py             |
         |  monitoring/alerts.py -> logs/    |
         +-----------------------------------+
```

## Run Locally in One Command

With Docker Compose:

```bash
docker compose up --build
```

Or directly with a local Python virtual environment:

```bash
python -m venv .venv && .venv/Scripts/pip install -r requirements.txt && .venv/Scripts/python orchestration/flow.py --date 2026-10-05 --orders data/raw/orders_day1.csv
```

## Run the Tests

Run all unit, warehouse idempotence, contract, monitoring, and mart tests with `pytest`:

```bash
python -m pytest
```

Run the dbt model builds and data tests:

```bash
dbt build --project-dir dbt --profiles-dir dbt
```

## How the Daily Schedule Works

The pipeline is orchestrated by **Prefect** in `orchestration/flow.py` (`shop_dataops_flow`). Each daily execution runs 9 sequential tasks:

1. `fetch-fx`: Fetches `EUR` exchange rates from Frankfurter for `--date` (defaulting to today's ISO date) with a 10s timeout and stores `payload["date"]`.
2. `read-orders`: Reads the target CSV file (`--orders`).
3. `check-schema`: Stops immediately if required columns are missing or if the file is empty.
4. `clean-orders`: Normalizes country names and flags invalid rows (missing customer, invalid date, non-positive quantity, unit price outside `0.01..1000`, duplicate `order_id`), saving rejected rows to `data/rejected/rejected_orders.csv`.
5. `check-reject-rate`: Stops if more than 20% of input rows were rejected.
6. `check-contracts`: Validates cleaned orders against `contracts/orders.yaml` and FX rates against `contracts/frankfurter.yaml`.
7. `upsert-warehouse`: Idempotently loads `raw_fx_rates` and `raw_orders` into `warehouse/shop.duckdb` using `PRIMARY KEY` + `INSERT OR REPLACE`.
8. `dbt-build`: Materializes `stg_orders`, `stg_fx_rates`, and `mart_revenue_daily`, and executes 21 data tests (including `not_null`, `unique`, `accepted_values`, and `assert_revenue_eur_not_negative`).
9. `monitoring-checks`: Verifies row counts and checks that an `EUR -> USD` rate exists for the latest `order_date`.

If any task or check fails, `monitoring/alerts.py` logs a timestamped alert to `logs/alerts.log`, prints it to stdout, and exits with code `1`. To run on a recurring daily schedule with Prefect, serve the flow with a cron schedule (e.g., `shop_dataops_flow.serve(name="daily-shop-dataops", cron="0 6 * * *")`) or trigger the Docker container daily via cron/Prefect worker.

# shop-dataops

End-to-end DataOps platform for a multi-currency online shop. Every day, the platform ingests raw shop orders from CSV files and exchange rates from the Frankfurter API (`https://api.frankfurter.dev/v1`, `EUR -> USD`), applies TP1 cleaning and quarantine rules, validates both sources against YAML data contracts, loads them idempotently into a DuckDB warehouse, builds layered dbt models (`staging` and `marts`) with 21 automated data tests, runs freshness and row-count monitoring checks, sends real alerts (`logs/alerts.log`, Slack/Discord webhook, and SMTP email), and serves an interactive **Streamlit Dashboard & Public Data Health Portal**.

## Architecture Diagram

```text
+------------------------+       +----------------------------------+
|   Shop Orders (CSV)    |       |   Frankfurter Exchange Rate API  |
| data/raw/orders_*.csv  |       | https://api.frankfurter.dev/v1   |
| (Days 1,4,5,6,7 valid; |       +-----------------+----------------+
|  Days 2,3 quarantined) |                         |
+-----------+------------+                         |
            |                                      v
            v                    +----------------------------------+
+------------------------+       |       FX Ingestion Module        |
|  TP1 Cleaning Pipeline |       |         ingestion/fx.py          |
|  pipeline/extract.py   |       |  (stores payload["date"] for FX) |
|  pipeline/validate.py  |       +-----------------+----------------+
|  pipeline/transform.py |                         |
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
              +------------+------------+
              |                         |
              v                         v
+---------------------------+ +-----------------------------------+
| Monitoring & Real Alerts  | |  Streamlit Dashboard & Innovation |
| monitoring/checks.py      | |  dashboard/app.py                 |
| monitoring/alerts.py      | |  - Q1, Q2, Q3 Charts (Plotly)     |
| -> logs/alerts.log        | |  - Public Data Health Portal      |
| -> Slack/Webhook & SMTP   | |  - AI/Statistical dbt Test Advisor|
+---------------------------+ +-----------------------------------+
```

## Run Locally in One Command

With Docker Compose (runs the 5-day flow runner and launches the Streamlit dashboard on `http://localhost:8501`):

```bash
docker compose up --build
```

Or directly with a local Python virtual environment:

```bash
# 1. Load all 5 valid sample days (Days 1, 4, 5, 6, 7) and build dbt models
.venv/Scripts/python orchestration/flow.py --all-valid-days

# 2. Launch the interactive Streamlit dashboard
.venv/Scripts/streamlit run dashboard/app.py
```

To run a single day:

```bash
.venv/Scripts/python orchestration/flow.py --date 2026-10-05 --orders data/raw/orders_day1.csv
```

## Run the Tests

Run all 13 unit, warehouse idempotence, contract, automated test advisor, monitoring, alerting, and mart tests with `pytest`:

```bash
.venv/Scripts/python -m pytest -v
```

Run the dbt model builds and 21 data tests:

```bash
.venv/Scripts/dbt build --project-dir dbt --profiles-dir dbt
```

## How the Daily Schedule Works

The pipeline is orchestrated by **Prefect** in `orchestration/flow.py` (`shop_dataops_flow`).

- **Scheduled Daily Execution (`flow.serve`)**: Start the persistent Prefect scheduler (defaults to `0 6 * * *`, every day at 06:00 UTC) with:
  ```bash
  .venv/Scripts/python orchestration/flow.py --serve --cron "0 6 * * *"
  ```
- **Alerting Channels (`monitoring/alerts.py`)**: Whenever a task or freshness check fails, `send_alert()` writes a timestamped entry to `logs/alerts.log`, prints to stdout, and automatically sends notifications to:
  - **Slack / Discord Webhook** when `ALERT_WEBHOOK_URL` is set in `.env` or GitHub Secrets.
  - **SMTP Email** when `ALERT_SMTP_HOST` and `ALERT_EMAIL_TO` (plus optional credentials) are set in `.env`.

## Innovations Beyond the Course ("Innovez")

1. **Public Data Health & Quarantine Status Portal (`dashboard/app.py` — Tab 2)**:
   - Gives business stakeholders real-time visibility into whether the warehouse passed freshness checks, how many rows were quarantined (`data/rejected/rejected_orders.csv`), and the live alert log (`logs/alerts.log`).
2. **Automated Statistical Data Profiler & dbt Test Advisor (`quality/advisor.py` & `dashboard/app.py` — Tab 3)**:
   - Automatically profiles `raw_orders` in DuckDB (min/max ranges, mean, and $3\sigma$ outlier thresholds) alongside `contracts/orders.yaml` and synthesizes ready-to-use `schema.yml` range tests and singular SQL outlier detection tests (`warn_revenue_outlier_3sigma`).

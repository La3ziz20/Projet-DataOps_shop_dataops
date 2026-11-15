# Project Report — shop-dataops

## Team Members

- Member 1: `<Last Name, First Name>`
- Member 2: `<Last Name, First Name>`

---

## 1. Architecture & Data Sources

<!-- Team notes: Explain the two data sources (Shop CSV orders and Frankfurter FX API) and how data flows from raw ingestion to the final mart. -->

- **Source 1 — Shop Orders (`data/raw/orders_*.csv` & `ingestion/orders_generator.py`)**:
  - Historical sample exports from TP1 (`orders_day1.csv`, `orders_day2.csv`, `orders_day3.csv`), additional multi-day exports (`orders_day4.csv` through `orders_day7.csv`), and an automated daily generator (`generate_daily_orders()`) for live scheduled runs.
- **Source 2 — Frankfurter Exchange Rate API (`ingestion/fx.py`)**:
  - Queries `https://api.frankfurter.dev/v1/{day}?from=EUR` with a 10-second timeout and `raise_for_status()`.
  - Stores `payload["date"]` (the effective ECB publication date returned by the API) rather than the requested calendar date, because weekend/holiday requests return the previous business day.

---

## 2. Cleaning Rules vs. Data Contracts

<!-- Team notes: Explain why TP1 cleaning rules in pipeline/transform.py are kept separate from YAML contract checks in quality/contracts.py, and what happens when each fails. -->

- **Step 1 — Cleaning & Quarantine (`pipeline/transform.py` & `pipeline/validate.py`)**:
  - Fixes deterministic formatting issues (`country.str.strip().str.title()`) and flags known row-level anomalies (`missing_customer_id`, `invalid_date`, `invalid_quantity`, `invalid_unit_price`, `duplicate_order_id`).
  - Quarantines bad rows in `data/rejected/rejected_orders.csv` for auditability and stops the pipeline (`check_reject_rate`) if more than **20%** of the file is rejected (e.g., `orders_day2.csv` where prices are exported in cents).
- **Step 2 — Post-Cleaning Contract Verification (`contracts/*.yaml` & `quality/contracts.py`)**:
  - Validates the cleaned `orders` DataFrame and the `frankfurter_fx` DataFrame against declarative YAML contracts.
  - Because cleaning has already run, any contract violation at this stage signals a code regression in `transform.py` or an upstream API contract change, immediately raising `DataQualityError` with the failing column name and count of bad rows.

---

## 3. Warehouse Idempotence

<!-- Team notes: Explain how PRIMARY KEY constraints and INSERT OR REPLACE in warehouse/store.py prevent duplicate rows when re-running the pipeline for the same day. -->

- `raw_fx_rates` defines `PRIMARY KEY (rate_date, base, currency)`.
- `raw_orders` defines `PRIMARY KEY (order_id)` and stores `revenue = ROUND(quantity * unit_price, 2)`.
- Both `upsert_fx()` and `upsert_orders()` deduplicate the incoming batch in-memory and execute `INSERT OR REPLACE INTO ...` inside DuckDB (`warehouse/shop.duckdb`). Re-running the pipeline multiple times for the same date overwrites existing records in place without creating duplicates (verified by `tests/test_warehouse.py`).

---

## 4. dbt Layering & Currency Conversion

<!-- Team notes: Explain the staging views (stg_orders, stg_fx_rates), the mart table (mart_revenue_daily), and why a LEFT JOIN is used on rate_date = order_date. -->

- **Staging layer (`models/staging/`)**:
  - `stg_orders.sql` (view): Explicit type casting on `raw_orders` (`BIGINT`, `DATE`, `VARCHAR`, `DOUBLE`).
  - `stg_fx_rates.sql` (view): Filters `raw_fx_rates` to `base = 'EUR'` and `currency = 'USD'`.
- **Mart layer (`models/marts/mart_revenue_daily.sql`)**:
  - Aggregates daily `revenue_eur` per `(order_date, country)` and joins `stg_fx_rates` using a **`LEFT JOIN` on `fx.rate_date = o.order_date`**.
  - Using a `LEFT JOIN` instead of an `INNER JOIN` ensures that if an exchange rate is missing for a given order date, the EUR revenue row is preserved with `revenue_usd = NULL` rather than silently disappearing from the executive report.

---

## 5. Orchestration, Monitoring, Alerting & Innovation Justification

<!-- Team notes: Explain the Prefect task order in orchestration/flow.py, the freshness checks in monitoring/checks.py, how alerts.py notifies Slack/email, and justify your innovation. -->

- **Orchestration (`orchestration/flow.py`)**:
  - Executes 9 Prefect `@task` steps in strict order: `fetch-fx` $\rightarrow$ `read-orders` $\rightarrow$ `check-schema` $\rightarrow$ `clean-orders` $\rightarrow$ `check-reject-rate` $\rightarrow$ `check-contracts` $\rightarrow$ `upsert-warehouse` $\rightarrow$ `dbt-build` $\rightarrow$ `monitoring-checks`.
  - Supports one-shot CLI runs (`--date`, `--orders`, `--all-valid-days`) and persistent scheduled runs (`--serve --cron "0 6 * * *"`).
- **Monitoring & Real Alerting (`monitoring/checks.py` & `monitoring/alerts.py`)**:
  - Verifies non-empty tables, reject rates, and FX rate freshness for `MAX(order_date)`.
  - On any failure, `send_alert()` appends a UTC timestamped alert to `logs/alerts.log` and dispatches notifications to Slack/Discord (`ALERT_WEBHOOK_URL`) and SMTP email (`ALERT_SMTP_HOST`, `ALERT_EMAIL_TO`).
- **Innovation Justification ("Innovez" — Public Data Health Portal + Automated Contract/dbt Test Advisor)**:
  - **What we built**:
    1. A **Public Data Health & Quarantine Portal** (Tab 2 of `dashboard/app.py`) exposing live warehouse freshness checks, quarantined rows (`rejected_orders.csv`), and alert logs directly alongside executive charts.
    2. An **Automated Statistical Profiler & dbt Test Advisor** (`quality/advisor.py` & Tab 3 of `dashboard/app.py`) that profiles `raw_orders` in DuckDB and combines empirical bounds with `contracts/orders.yaml` to generate dbt `schema.yml` range tests and singular SQL outlier tests (`warn_revenue_outlier_3sigma`).
  - **What it catches vs. what it misses (Critical Limitations)**:
    - *Catches*: Sudden order-volume spikes (`quantity > 2 * historical_max`) and extreme high-value line items (`revenue > mean + 3 * std`) that technically pass static contract bounds (`unit_price <= 1000`, `quantity >= 1`) — for instance, a bulk order of 10 office chairs (€4,490) that is schema-valid but statistically unusual.
    - *Misses / Limitations*: On a small sample window (5 days / 55 clean orders), parametric $3\sigma$ thresholds have high variance and assume a roughly normal revenue distribution, whereas e-commerce order values are right-skewed and multimodal (e.g., €19.90 mice vs. €449.00 chairs). Therefore, generated $3\sigma$ tests are configured with `severity: warn` rather than hard-failing the build, while deterministic YAML contract rules remain the hard gate (`severity: error`).
  - **Why we chose it over alternatives**:
    - Unlike an external LLM API call that requires a paid API key and introduces non-deterministic outputs in CI, a local statistical + contract-driven test synthesizer is **100% deterministic, zero-secret, fast in CI**, and directly bridges `contracts/orders.yaml` with `dbt/models/staging/schema.yml`.

---

## 6. Business Questions Results (Q1, Q2, Q3)

<!-- Verified results across the 5 valid days loaded in warehouse/shop.duckdb (2026-10-05, 2026-10-08, 2026-10-09, 2026-10-12, 2026-10-13) -->

### Q1: Total revenue per country in EUR

| country | total_revenue_eur |
| :--- | ---: |
| Italy | 7857.90 |
| France | 5978.50 |
| Morocco | 5654.00 |
| Germany | 5040.50 |
| Tunisia | 5021.00 |

### Q2: Best day by revenue in USD

| order_date | total_revenue_usd |
| :--- | ---: |
| 2026-10-09 | 9979.51 |

### Q3: Share of total revenue per country, in percent

| country | revenue_eur | share_pct |
| :--- | ---: | ---: |
| Italy | 7857.90 | 26.59 |
| France | 5978.50 | 20.23 |
| Morocco | 5654.00 | 19.13 |
| Germany | 5040.50 | 17.06 |
| Tunisia | 5021.00 | 16.99 |

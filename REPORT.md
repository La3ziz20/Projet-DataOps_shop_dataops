# Project Report — shop-dataops

## Team Members

- Member 1:
- Member 2:

## 1. Architecture & Data Sources

<!-- Explain the two data sources (Shop CSV orders and Frankfurter FX API) and how data flows from raw ingestion to the final mart. -->

## 2. Cleaning Rules vs. Data Contracts

<!-- Explain why TP1 cleaning rules in pipeline/transform.py are kept separate from YAML contract checks in quality/contracts.py, and what happens when each fails. -->

## 3. Warehouse Idempotence

<!-- Explain how PRIMARY KEY constraints and INSERT OR REPLACE in warehouse/store.py prevent duplicate rows when re-running the pipeline for the same day. -->

## 4. dbt Layering & Currency Conversion

<!-- Explain the staging views (stg_orders, stg_fx_rates), the mart table (mart_revenue_daily), and why a LEFT JOIN is used on rate_date = order_date. -->

## 5. Orchestration, Monitoring & Alerting

<!-- Explain the Prefect task order in orchestration/flow.py, the freshness checks in monitoring/checks.py, and how alerts.py can be extended to Slack or email. -->

## 6. Business Questions Results (Q1, Q2, Q3)

<!-- Paste and interpret the results of Q1 (revenue per country in EUR), Q2 (best day in USD), and Q3 (country share of total revenue in percent). -->

import os
from pathlib import Path

import duckdb
import pandas as pd

# Default path to the DuckDB warehouse file, configurable via environment variable
DEFAULT_DB_PATH = os.getenv("WAREHOUSE_DB_PATH", "warehouse/shop.duckdb")


def connect(db_path: str = DEFAULT_DB_PATH) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection and ensure raw tables exist with primary keys."""
    path = Path(db_path)
    # Create the parent directory if it does not exist yet
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))

    # Composite primary key (rate_date, base, currency) guarantees idempotence for FX rates
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_fx_rates (
            rate_date DATE,
            base VARCHAR,
            currency VARCHAR,
            rate DOUBLE,
            PRIMARY KEY (rate_date, base, currency)
        )
        """
    )

    # Primary key on order_id guarantees idempotence for orders
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_orders (
            order_id BIGINT PRIMARY KEY,
            order_date DATE,
            customer_id VARCHAR,
            country VARCHAR,
            product VARCHAR,
            quantity BIGINT,
            unit_price DOUBLE,
            revenue DOUBLE
        )
        """
    )
    return con


def upsert_fx(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """Idempotently upsert FX rate rows into raw_fx_rates."""
    if df.empty:
        return 0
    # Deduplicate input batch on the composite primary key, keeping the latest entry
    staging = df[["rate_date", "base", "currency", "rate"]].drop_duplicates(
        subset=["rate_date", "base", "currency"], keep="last"
    ).copy()
    # Normalize rate_date to ISO date string so DuckDB casts cleanly to DATE
    staging["rate_date"] = pd.to_datetime(staging["rate_date"]).dt.strftime("%Y-%m-%d")

    con.register("fx_staging", staging)
    # INSERT OR REPLACE overwrites existing primary key matches without creating duplicates
    con.execute(
        """
        INSERT OR REPLACE INTO raw_fx_rates (rate_date, base, currency, rate)
        SELECT CAST(rate_date AS DATE), base, currency, rate
        FROM fx_staging
        """
    )
    con.unregister("fx_staging")
    return len(staging)


def upsert_orders(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    """Idempotently upsert cleaned orders into raw_orders with computed revenue."""
    if df.empty:
        return 0
    staging = df.copy()
    # Compute revenue as quantity * unit_price as required by business rules
    staging["revenue"] = (staging["quantity"] * staging["unit_price"]).round(2)
    # Normalize order_date to ISO date string so DuckDB casts cleanly to DATE
    staging["order_date"] = pd.to_datetime(staging["order_date"]).dt.strftime("%Y-%m-%d")
    # Deduplicate input batch on order_id primary key
    staging = staging[
        [
            "order_id",
            "order_date",
            "customer_id",
            "country",
            "product",
            "quantity",
            "unit_price",
            "revenue",
        ]
    ].drop_duplicates(subset=["order_id"], keep="first")

    con.register("orders_staging", staging)
    # INSERT OR REPLACE overwrites existing order_id rows idempotently
    con.execute(
        """
        INSERT OR REPLACE INTO raw_orders (
            order_id, order_date, customer_id, country, product, quantity, unit_price, revenue
        )
        SELECT
            CAST(order_id AS BIGINT),
            CAST(order_date AS DATE),
            customer_id,
            country,
            product,
            CAST(quantity AS BIGINT),
            CAST(unit_price AS DOUBLE),
            CAST(revenue AS DOUBLE)
        FROM orders_staging
        """
    )
    con.unregister("orders_staging")
    return len(staging)

import pandas as pd

from pipeline.extract import read_orders
from pipeline.transform import clean_orders
from warehouse.store import connect, upsert_fx, upsert_orders


def test_mart_totals_equal_sum_of_clean_orders(tmp_path):
    """Mart EUR revenue total matches the sum of clean orders from orders_day1.csv."""
    raw = read_orders("data/raw/orders_day1.csv")
    clean, _ = clean_orders(raw)
    expected_total_eur = round((clean["quantity"] * clean["unit_price"]).sum(), 2)

    db_file = tmp_path / "mart_test.duckdb"
    con = connect(str(db_file))
    try:
        fx_df = pd.DataFrame(
            [{"rate_date": "2026-10-05", "base": "EUR", "currency": "USD", "rate": 1.1204}]
        )
        upsert_fx(con, fx_df)
        upsert_orders(con, clean)

        # Execute the same aggregation logic as mart_revenue_daily on the loaded warehouse
        mart_df = con.execute(
            """
            WITH daily_orders AS (
                SELECT order_date, country, ROUND(SUM(revenue), 2) AS revenue_eur
                FROM raw_orders
                GROUP BY order_date, country
            )
            SELECT
                o.order_date,
                o.country,
                o.revenue_eur,
                ROUND(o.revenue_eur * fx.rate, 2) AS revenue_usd
            FROM daily_orders o
            LEFT JOIN raw_fx_rates fx
                ON fx.rate_date = o.order_date AND fx.base = 'EUR' AND fx.currency = 'USD'
            """
        ).df()

        assert round(mart_df["revenue_eur"].sum(), 2) == expected_total_eur
        assert mart_df["revenue_usd"].notna().all()
    finally:
        con.close()

import pandas as pd

from monitoring.checks import run_checks
from warehouse.store import connect, upsert_fx, upsert_orders


def test_upsert_fx_is_idempotent(tmp_path):
    """Loading the same FX rows twice gives the same row count."""
    db_file = tmp_path / "test_shop.duckdb"
    con = connect(str(db_file))
    try:
        fx_df = pd.DataFrame(
            [
                {"rate_date": "2026-10-05", "base": "EUR", "currency": "USD", "rate": 1.1204},
                {"rate_date": "2026-10-05", "base": "EUR", "currency": "GBP", "rate": 0.8472},
            ]
        )
        upsert_fx(con, fx_df)
        count_first = con.execute("SELECT COUNT(*) FROM raw_fx_rates").fetchall()[0][0]

        # Second load of the identical rows must not create duplicates
        upsert_fx(con, fx_df)
        count_second = con.execute("SELECT COUNT(*) FROM raw_fx_rates").fetchall()[0][0]

        assert count_first == 2
        assert count_second == 2
    finally:
        con.close()


def test_upsert_orders_is_idempotent(tmp_path):
    """Loading the same cleaned orders twice gives the same row count and computes revenue."""
    db_file = tmp_path / "test_shop.duckdb"
    con = connect(str(db_file))
    try:
        orders_df = pd.DataFrame(
            [
                {
                    "order_id": 1001,
                    "order_date": "2026-10-05",
                    "customer_id": "C036",
                    "country": "Italy",
                    "product": "Office chair",
                    "quantity": 4,
                    "unit_price": 449.0,
                }
            ]
        )
        upsert_orders(con, orders_df)
        upsert_orders(con, orders_df)

        row = con.execute("SELECT COUNT(*), SUM(revenue) FROM raw_orders").fetchall()[0]
        assert row[0] == 1
        assert row[1] == 1796.0
    finally:
        con.close()


def test_monitoring_check_flags_empty_table(tmp_path):
    """Monitoring check flags an empty warehouse table."""
    db_file = tmp_path / "empty_shop.duckdb"
    con = connect(str(db_file))
    try:
        problems = run_checks(con)
        assert any("No orders loaded" in p for p in problems)
        assert any("No FX rates loaded" in p for p in problems)
    finally:
        con.close()

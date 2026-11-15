import pandas as pd
import pytest

from pipeline.validate import DataQualityError
from quality.advisor import generate_dbt_test_recommendations
from quality.contracts import check_contract
from warehouse.store import connect, upsert_orders


def test_contract_fails_on_bad_quantity_zero():
    """A bad quantity of 0 fails the orders contract and lists the failing column and count."""
    bad_orders = pd.DataFrame(
        [
            {
                "order_id": 1001,
                "order_date": "2026-10-05",
                "customer_id": "C001",
                "country": "France",
                "product": "Mouse",
                "quantity": 0,
                "unit_price": 19.9,
            }
        ]
    )
    with pytest.raises(DataQualityError) as exc_info:
        check_contract(bad_orders, "contracts/orders.yaml")

    assert "quantity" in str(exc_info.value)
    assert "1 bad rows" in str(exc_info.value)


def test_contract_passes_on_valid_fx_and_orders():
    """Valid orders and Frankfurter FX rows pass their respective contracts."""
    valid_orders = pd.DataFrame(
        [
            {
                "order_id": 1001,
                "order_date": "2026-10-05",
                "customer_id": "C001",
                "country": "France",
                "product": "Mouse",
                "quantity": 2,
                "unit_price": 19.9,
            }
        ]
    )
    valid_fx = pd.DataFrame(
        [
            {
                "rate_date": "2026-10-05",
                "base": "EUR",
                "currency": "USD",
                "rate": 1.1204,
            }
        ]
    )
    check_contract(valid_orders, "contracts/orders.yaml")
    check_contract(valid_fx, "contracts/frankfurter.yaml")


def test_advisor_generates_dbt_recommendations(tmp_path):
    """Automated test advisor profiles raw_orders and recommends dbt schema/singular tests."""
    db_file = tmp_path / "advisor.duckdb"
    con = connect(str(db_file))
    try:
        orders = pd.DataFrame(
            [
                {
                    "order_id": 1001,
                    "order_date": "2026-10-05",
                    "customer_id": "C001",
                    "country": "France",
                    "product": "Mouse",
                    "quantity": 2,
                    "unit_price": 50.0,
                },
                {
                    "order_id": 1002,
                    "order_date": "2026-10-05",
                    "customer_id": "C002",
                    "country": "Italy",
                    "product": "Monitor",
                    "quantity": 3,
                    "unit_price": 200.0,
                },
            ]
        )
        upsert_orders(con, orders)
        rec = generate_dbt_test_recommendations(con)
        assert rec["profile"]["row_count"] == 2
        assert "models" in rec["suggested_dbt_tests"]
        assert "singular_sql_tests" in rec["suggested_dbt_tests"]
    finally:
        con.close()


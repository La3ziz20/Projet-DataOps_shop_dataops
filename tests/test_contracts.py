import pandas as pd
import pytest

from pipeline.validate import DataQualityError
from quality.contracts import check_contract


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

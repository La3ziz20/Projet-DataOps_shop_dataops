import pandas as pd
import pytest

from pipeline.validate import DataQualityError
from quality.contracts import check_contract


def test_contract_fails_on_wrong_type_integer():
    """A column defined as integer should fail when given a float value"""
    df = pd.DataFrame([
        {
            "order_id": 1002,
            "order_date": "2026-10-05",
            "customer_id": "C002",
            "country": "Germany",
            "product": "Keyboard",
            "quantity": 2.5,
            "unit_price": 49.99,
        }
    ])
    # orders.yaml now has type integer for quantity
    with pytest.raises(DataQualityError) as exc_info:
        check_contract(df, "contracts/orders.yaml")
    assert "quantity" in str(exc_info.value)
    assert "type integer" in str(exc_info.value)


def test_contract_passes_with_correct_types():
    """A valid dataframe respecting all type rules should pass"""
    df = pd.DataFrame([
        {
            "order_id": 1003,
            "order_date": "2026-10-08",
            "customer_id": "C003",
            "country": "Italy",
            "product": "Monitor",
            "quantity": 1,
            "unit_price": 199.99,
        }
    ])
    # Should not raise
    check_contract(df, "contracts/orders.yaml")


def test_contract_without_type_rule_behaves_as_before():
    """If a column has no type rule, old constraints still apply"""
    # Remove type from quantity temporarily by loading contract dict and deleting entry
    import yaml
    from quality.contracts import load_contract
    contract = load_contract("contracts/orders.yaml")
    del contract["columns"]["quantity"]["type"]
    df = pd.DataFrame([
        {
            "order_id": 1004,
            "order_date": "2026-10-08",
            "customer_id": "C004",
            "country": "France",
            "product": "Mouse",
            "quantity": 0,
            "unit_price": 19.99,
        }
    ])
    with pytest.raises(DataQualityError) as exc_info:
        check_contract(df, contract)
    # Should report quantity null/min etc., not type
    assert "quantity" in str(exc_info.value)
    assert "type" not in str(exc_info.value)

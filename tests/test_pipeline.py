import pandas as pd
import pytest

from ingestion.orders_generator import generate_daily_orders
from pipeline.transform import clean_orders, revenue_by_country
from pipeline.validate import DataQualityError, check_schema


def make_orders(rows):
    """Build a small orders table. Each row only gives the columns it wants to change."""
    default = {
        "order_id": 1,
        "order_date": "2026-10-05",
        "customer_id": "C001",
        "country": "Tunisia",
        "product": "Mouse",
        "quantity": 1,
        "unit_price": 10.0,
    }
    # Merge default fields with per-row overrides
    return pd.DataFrame([{**default, **row} for row in rows])


def test_revenue_by_country():
    orders = make_orders(
        [
            {"order_id": 1, "country": "Tunisia", "quantity": 2, "unit_price": 10.0},
            {"order_id": 2, "country": "Tunisia", "quantity": 1, "unit_price": 5.0},
            {"order_id": 3, "country": "France", "quantity": 3, "unit_price": 1.0},
        ]
    )
    report = revenue_by_country(orders)
    assert report.set_index("country")["revenue"].to_dict() == {"France": 3.0, "Tunisia": 25.0}


def test_negative_quantity_is_rejected():
    orders = make_orders([{"order_id": 1, "quantity": -2}, {"order_id": 2, "quantity": 3}])
    clean, rejected = clean_orders(orders)
    assert list(clean["order_id"]) == [2]
    assert list(rejected["reject_reason"]) == ["invalid_quantity"]


def test_duplicate_order_is_rejected():
    orders = make_orders([{"order_id": 1}, {"order_id": 1}, {"order_id": 2}])
    clean, rejected = clean_orders(orders)
    assert list(clean["order_id"]) == [1, 2]
    assert list(rejected["reject_reason"]) == ["duplicate_order_id"]


def test_country_is_normalised():
    orders = make_orders(
        [
            {"order_id": 1, "country": " france"},
            {"order_id": 2, "country": "FRANCE "},
        ]
    )
    clean, rejected = clean_orders(orders)
    assert list(clean["country"]) == ["France", "France"]
    assert len(rejected) == 0


def test_check_schema_stops_on_missing_column():
    orders = make_orders([{"order_id": 1}]).rename(columns={"unit_price": "price"})
    with pytest.raises(DataQualityError):
        check_schema(orders)


def test_daily_orders_generator_creates_valid_csv(tmp_path):
    """Automated daily generator creates a valid orders CSV for any new scheduled date."""
    csv_path = generate_daily_orders("2026-10-14", output_dir=tmp_path)
    assert csv_path.exists()
    df = pd.read_csv(csv_path)
    check_schema(df)
    clean, rejected = clean_orders(df)
    assert len(clean) == len(df)
    assert len(rejected) == 0


"""Innovation module: Automated data profiler and dbt test advisor."""

from pathlib import Path
from typing import Any

import duckdb
import yaml


def generate_dbt_test_recommendations(
    con: duckdb.DuckDBPyConnection,
    contract_path: str | Path = "contracts/orders.yaml",
) -> dict[str, Any]:
    """Profile raw_orders against the YAML contract and synthesize dbt test recommendations."""
    contract = yaml.safe_load(Path(contract_path).read_text(encoding="utf-8"))
    rows = con.execute(
        """
        SELECT
            COUNT(*) AS row_count,
            MIN(quantity) AS min_qty,
            MAX(quantity) AS max_qty,
            MIN(unit_price) AS min_price,
            MAX(unit_price) AS max_price,
            AVG(revenue) AS avg_revenue,
            STDDEV_POP(revenue) AS std_revenue
        FROM raw_orders
        """
    ).fetchall()

    row = rows[0] if rows else (0, 0, 0, 0.0, 0.0, 0.0, 0.0)
    row_count = int(row[0] or 0)
    max_qty = int(row[2] or 10)
    avg_rev = float(row[5] or 0.0)
    std_rev = float(row[6] or 0.0)
    # 3-sigma anomaly threshold for single-order revenue
    upper_3sigma = round(avg_rev + 3.0 * std_rev, 2) if row_count > 1 else 5000.0

    allowed_countries = (
        contract.get("columns", {}).get("country", {}).get("allowed_values", [])
    )

    suggested_yaml = {
        "version": 2,
        "models": [
            {
                "name": "stg_orders",
                "columns": [
                    {"name": "order_id", "tests": ["not_null", "unique"]},
                    {
                        "name": "country",
                        "tests": [
                            "not_null",
                            {"accepted_values": {"arguments": {"values": allowed_countries}}},
                        ],
                    },
                    {
                        "name": "quantity",
                        "tests": [
                            "not_null",
                            {
                                "recommended_range_check": {
                                    "min_value": 1,
                                    "max_value": max(10, max_qty * 2),
                                }
                            },
                        ],
                    },
                ],
            }
        ],
        "singular_sql_tests": [
            {
                "name": "warn_revenue_outlier_3sigma",
                "sql": (
                    f"SELECT order_id, revenue FROM {{{{ ref('stg_orders') }}}} "
                    f"WHERE revenue > {upper_3sigma}"
                ),
            }
        ],
    }

    return {
        "profile": {
            "row_count": row_count,
            "max_qty": max_qty,
            "avg_revenue": round(avg_rev, 2),
            "upper_3sigma_revenue": upper_3sigma,
        },
        "suggested_dbt_tests": suggested_yaml,
    }

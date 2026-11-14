from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from pipeline.validate import DataQualityError


def load_contract(contract_path: str | Path) -> dict[str, Any]:
    """Load a YAML data contract from disk."""
    path = Path(contract_path)
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def check_contract(df: pd.DataFrame, contract_or_path: str | Path | dict[str, Any]) -> None:
    """Validate a DataFrame against a YAML contract; raise DataQualityError on any violation."""
    # Accept either an already-loaded dict or a path to the YAML contract file
    contract = (
        contract_or_path
        if isinstance(contract_or_path, dict)
        else load_contract(contract_or_path)
    )
    columns_spec = contract.get("columns", {})

    if len(df) == 0:
        raise DataQualityError("Contract violation: DataFrame is empty (0 rows)")

    violations: list[str] = []

    for col_name, rules in columns_spec.items():
        # Verify required column exists in the DataFrame
        if col_name not in df.columns:
            violations.append(f"{col_name}: missing column ({len(df)} bad rows)")
            continue

        series = df[col_name]

        # Check not_null constraint
        if rules.get("not_null", False):
            bad_nulls = int(series.isna().sum())
            if bad_nulls > 0:
                violations.append(f"{col_name}: null values ({bad_nulls} bad rows)")

        # Check uniqueness constraint
        if rules.get("unique", False):
            bad_dups = int(series.duplicated(keep="first").sum())
            if bad_dups > 0:
                violations.append(f"{col_name}: duplicate values ({bad_dups} bad rows)")

        # Check minimum numeric threshold (including nulls as bad if not already caught)
        if "min" in rules:
            min_val = rules["min"]
            numeric = pd.to_numeric(series, errors="coerce")
            bad_min = int((numeric.isna() | (numeric < min_val)).sum())
            if bad_min > 0:
                violations.append(f"{col_name}: below min {min_val} ({bad_min} bad rows)")

        # Check maximum numeric threshold
        if "max" in rules:
            max_val = rules["max"]
            numeric = pd.to_numeric(series, errors="coerce")
            bad_max = int((numeric.isna() | (numeric > max_val)).sum())
            if bad_max > 0:
                violations.append(f"{col_name}: above max {max_val} ({bad_max} bad rows)")

        # Check allowed categorical values
        if "allowed_values" in rules:
            allowed = set(rules["allowed_values"])
            bad_allowed = int((~series.isin(allowed)).sum())
            if bad_allowed > 0:
                violations.append(
                    f"{col_name}: outside allowed_values {sorted(allowed)} ({bad_allowed} bad rows)"
                )

    if violations:
        raise DataQualityError("Contract check failed -> " + "; ".join(violations))

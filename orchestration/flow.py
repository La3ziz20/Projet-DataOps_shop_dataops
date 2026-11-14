import argparse
import os
import shutil
import subprocess
import sys
from datetime import date as dt_date
from pathlib import Path

# Disable Prefect telemetry heartbeat to avoid SQLite lock contention on ephemeral local runs
os.environ.setdefault("PREFECT_SERVER_ANALYTICS_ENABLED", "false")

# Add the project root to sys.path so `python orchestration/flow.py` resolves local packages
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from prefect import flow, task

from ingestion.fx import fetch_rates
from monitoring.alerts import send_alert
from monitoring.checks import run_checks
from pipeline.extract import read_orders
from pipeline.load import write_csv
from pipeline.transform import clean_orders
from pipeline.validate import DataQualityError, check_reject_rate, check_schema
from quality.contracts import check_contract
from warehouse.store import DEFAULT_DB_PATH, connect, upsert_fx, upsert_orders

REJECTED_PATH = "data/rejected/rejected_orders.csv"
ORDERS_CONTRACT_PATH = "contracts/orders.yaml"
FX_CONTRACT_PATH = "contracts/frankfurter.yaml"


@task(name="fetch-fx")
def task_fetch_fx(day: str) -> pd.DataFrame:
    """Task 1: Fetch exchange rates from the Frankfurter API for the target date."""
    return fetch_rates(day, base="EUR")


@task(name="read-orders")
def task_read_orders(orders_path: str) -> pd.DataFrame:
    """Task 2: Read raw order rows from CSV."""
    return read_orders(orders_path)


@task(name="check-schema")
def task_check_schema(raw_orders: pd.DataFrame) -> None:
    """Task 3: Validate the input schema and non-emptiness."""
    check_schema(raw_orders)


@task(name="clean-orders")
def task_clean_orders(raw_orders: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Task 4: Apply TP1 cleaning rules and quarantine rejected rows if any."""
    clean, rejected = clean_orders(raw_orders)
    if len(rejected) > 0:
        write_csv(rejected, REJECTED_PATH)
    return clean, rejected


@task(name="check-reject-rate")
def task_check_reject_rate(n_total: int, n_rejected: int) -> None:
    """Task 5: Stop the pipeline if the share of rejected rows exceeds MAX_REJECT_RATE."""
    check_reject_rate(n_total, n_rejected)


@task(name="check-contracts")
def task_check_contracts(clean_df: pd.DataFrame, fx_df: pd.DataFrame) -> None:
    """Task 6: Verify both cleaned orders and FX rates against their YAML data contracts."""
    check_contract(clean_df, ORDERS_CONTRACT_PATH)
    check_contract(fx_df, FX_CONTRACT_PATH)


@task(name="upsert-warehouse")
def task_upsert_warehouse(
    clean_df: pd.DataFrame, fx_df: pd.DataFrame, db_path: str = DEFAULT_DB_PATH
) -> tuple[int, int]:
    """Task 7: Idempotently upsert FX rates and cleaned orders into DuckDB."""
    con = connect(db_path)
    try:
        n_fx = upsert_fx(con, fx_df)
        n_orders = upsert_orders(con, clean_df)
        return n_fx, n_orders
    finally:
        con.close()


@task(name="dbt-build")
def task_dbt_build(db_path: str = DEFAULT_DB_PATH) -> str:
    """Task 8: Execute `dbt build` to materialize staging/marts and run schema/singular tests."""
    # Locate the dbt executable in the current Python environment or system PATH
    dbt_bin = shutil.which("dbt") or str(Path(sys.executable).parent / "dbt")
    cmd = [dbt_bin, "build", "--project-dir", "dbt", "--profiles-dir", "dbt"]
    env_Copy = dict(**__import__("os").environ)
    env_Copy["WAREHOUSE_DB_PATH"] = db_path

    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        env=env_Copy,
        capture_output=True,
        text=True,
        check=False,
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"dbt build failed (exit {result.returncode}):\n{result.stdout}\n{result.stderr}")
    return result.stdout


@task(name="monitoring-checks")
def task_monitoring_checks(
    n_total: int, n_rejected: int, db_path: str = DEFAULT_DB_PATH
) -> list[str]:
    """Task 9: Run warehouse freshness and row count checks."""
    con = connect(db_path)
    try:
        return run_checks(con, n_total=n_total, n_rejected=n_rejected)
    finally:
        con.close()


@flow(name="shop-dataops-daily-flow", log_prints=True)
def shop_dataops_flow(
    run_date: str = "2026-10-05",
    orders_path: str = "data/raw/orders_day1.csv",
    db_path: str = DEFAULT_DB_PATH,
) -> dict[str, int]:
    """Orchestrate daily ingestion, cleaning, contract checks, DuckDB load, dbt build, and monitoring."""
    try:
        print(f"Starting shop-dataops flow for date={run_date}, orders={orders_path}")
        fx_df = task_fetch_fx(run_date)
        raw_df = task_read_orders(orders_path)
        task_check_schema(raw_df)
        clean_df, rejected_df = task_clean_orders(raw_df)
        print(
            f"Orders cleaned: {len(clean_df)} clean rows, {len(rejected_df)} rejected rows (out of {len(raw_df)})"
        )
        task_check_reject_rate(len(raw_df), len(rejected_df))
        task_check_contracts(clean_df, fx_df)
        n_fx, n_orders = task_upsert_warehouse(clean_df, fx_df, db_path=db_path)
        print(f"Upserted {n_fx} FX rows and {n_orders} order rows into {db_path}")
        task_dbt_build(db_path=db_path)
        problems = task_monitoring_checks(len(raw_df), len(rejected_df), db_path=db_path)
        if problems:
            problem_summary = "; ".join(problems)
            raise DataQualityError(f"Monitoring checks failed: {problem_summary}")
        print("Monitoring checks passed with 0 problems.")
        return {"fx_rows": n_fx, "order_rows": n_orders, "rejected_rows": len(rejected_df)}
    except Exception as exc:
        # Send alert to logs/alerts.log and stdout whenever any task or freshness check fails
        send_alert(f"Pipeline failed for date={run_date}, orders={orders_path}: {exc}")
        raise


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments with defaults for date and orders path."""
    parser = argparse.ArgumentParser(description="Run the shop-dataops Prefect flow.")
    parser.add_argument(
        "--date",
        default=dt_date.today().isoformat(),
        help="Target date in YYYY-MM-DD format (default: today).",
    )
    parser.add_argument(
        "--orders",
        default="data/raw/orders_day1.csv",
        help="Path to the input orders CSV file.",
    )
    parser.add_argument(
        "--db-path",
        default=DEFAULT_DB_PATH,
        help="Path to the DuckDB warehouse file.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    try:
        shop_dataops_flow(run_date=args.date, orders_path=args.orders, db_path=args.db_path)
    except Exception:
        # Exit with a non-zero status code when the flow fails
        sys.exit(1)

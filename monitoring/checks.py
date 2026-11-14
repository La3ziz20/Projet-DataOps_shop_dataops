import duckdb

from pipeline.validate import MAX_REJECT_RATE


def run_checks(
    con: duckdb.DuckDBPyConnection,
    n_total: int | None = None,
    n_rejected: int = 0,
    max_reject_rate: float = MAX_REJECT_RATE,
) -> list[str]:
    """Check warehouse freshness, row counts, and reject rate. Return a list of problems."""
    problems: list[str] = []

    # Check reject rate if batch counts were provided
    if n_total is not None and n_total > 0:
        reject_rate = n_rejected / n_total
        if reject_rate > max_reject_rate:
            problems.append(
                f"Reject rate {reject_rate:.0%} ({n_rejected}/{n_total}) exceeds limit {max_reject_rate:.0%}"
            )

    # Check raw_orders row count and latest order_date
    orders_row = con.execute(
        "SELECT COUNT(*), MAX(order_date) FROM raw_orders"
    ).fetchone()
    orders_count = orders_row[0] if orders_row else 0
    latest_order_date = orders_row[1] if orders_row else None

    if orders_count == 0:
        problems.append("No orders loaded in raw_orders (0 rows)")

    # Check raw_fx_rates row count and latest rate_date
    fx_row = con.execute(
        "SELECT COUNT(*), MAX(rate_date) FROM raw_fx_rates"
    ).fetchone()
    fx_count = fx_row[0] if fx_row else 0
    latest_rate_date = fx_row[1] if fx_row else None

    if fx_count == 0:
        problems.append("No FX rates loaded in raw_fx_rates (0 rows)")

    # Freshness check: verify an EUR->USD FX rate exists for the latest order_date
    if latest_order_date is not None:
        matching_fx = con.execute(
            """
            SELECT COUNT(*)
            FROM raw_fx_rates
            WHERE rate_date = ? AND base = 'EUR' AND currency = 'USD'
            """,
            [latest_order_date],
        ).fetchone()[0]
        if matching_fx == 0:
            problems.append(
                f"No FX rate for the latest order date ({latest_order_date}); latest rate_date is {latest_rate_date}"
            )

    return problems

import hashlib
import random
from pathlib import Path

import pandas as pd

COUNTRIES = ["France", "Germany", "Italy", "Morocco", "Tunisia"]
CATALOG = [
    ("Office chair", 449.0),
    ("Monitor", 229.0),
    ("Headset", 59.5),
    ("Keyboard", 45.0),
    ("Webcam", 39.0),
    ("Desk lamp", 29.0),
    ("Mouse", 19.9),
]


def generate_daily_orders(
    target_date: str,
    output_dir: str | Path = "data/raw",
    min_orders: int = 8,
    max_orders: int = 16,
) -> Path:
    """Deterministically generate a realistic daily shop orders CSV for `target_date` if none exists."""
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"orders_{target_date}.csv"

    if out_path.exists():
        return out_path

    # Seed RNG from the date string so re-running for the same date is reproducible
    seed = int(hashlib.sha256(target_date.encode("utf-8")).hexdigest()[:8], 16)
    rng = random.Random(seed)

    # Derive a unique base order_id range from the date (YYYYMMDD * 100)
    compact_date = int(target_date.replace("-", ""))
    base_order_id = compact_date * 100

    n_rows = rng.randint(min_orders, max_orders)
    rows = []
    for idx in range(1, n_rows + 1):
        product, unit_price = rng.choice(CATALOG)
        rows.append(
            {
                "order_id": base_order_id + idx,
                "order_date": target_date,
                "customer_id": f"C{rng.randint(1, 50):03d}",
                "country": rng.choice(COUNTRIES),
                "product": product,
                "quantity": rng.randint(1, 5),
                "unit_price": unit_price,
            }
        )

    df = pd.DataFrame(rows)
    df.to_csv(out_path, index=False)
    return out_path

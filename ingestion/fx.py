import os

import pandas as pd
import requests

# Single place for the Frankfurter API base URL (modern host: https://api.frankfurter.dev/v1)
BASE_URL = os.getenv("FRANKFURTER_BASE_URL", "https://api.frankfurter.dev/v1")
TIMEOUT_SECONDS = 10


def fetch_rates(day: str, base: str = "EUR") -> pd.DataFrame:
    """Fetch exchange rates for a given YYYY-MM-DD day and base currency."""
    # Build the date-specific endpoint URL
    url = f"{BASE_URL}/{day}"
    response = requests.get(url, params={"from": base}, timeout=TIMEOUT_SECONDS)
    # Fail immediately on HTTP 4xx or 5xx responses
    response.raise_for_status()
    payload = response.json()

    # Store the effective date returned by the API (weekends return the previous business day)
    rate_date = payload["date"]
    base_currency = payload.get("base", base)
    rates = payload.get("rates", {})

    # Flatten the rates dictionary into one row per target currency
    rows = [
        {
            "rate_date": rate_date,
            "base": base_currency,
            "currency": str(currency),
            "rate": float(rate),
        }
        for currency, rate in rates.items()
    ]
    return pd.DataFrame(rows, columns=["rate_date", "base", "currency", "rate"])

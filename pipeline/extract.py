import pandas as pd


def read_orders(path):
    """Extract: read the raw file exactly as it is."""
    # Read CSV directly into a DataFrame without altering raw values
    return pd.read_csv(path)

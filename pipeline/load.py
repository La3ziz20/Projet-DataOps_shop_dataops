from pathlib import Path


def write_csv(df, path):
    """Load: write a table to a CSV file."""
    # Convert string path to Path object to ensure parent directories exist
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)

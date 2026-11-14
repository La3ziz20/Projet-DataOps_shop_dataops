import os
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_ALERT_LOG = os.getenv("ALERT_LOG_PATH", "logs/alerts.log")


def send_alert(message: str, log_path: str | Path = DEFAULT_ALERT_LOG) -> str:
    """Write a timestamped alert to logs/alerts.log and print it.

    Single entry point so email (SMTP) or Slack webhook integration can be added here later.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    formatted = f"[{timestamp}] ALERT: {message}"

    # Ensure the log directory exists before appending the alert line
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(formatted + "\n")

    print(formatted)
    # Extension hook: add _send_slack_webhook(formatted) or _send_email(formatted) here later
    return formatted

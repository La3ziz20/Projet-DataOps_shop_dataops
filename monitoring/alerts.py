import os
import smtplib
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path

import requests

DEFAULT_ALERT_LOG = os.getenv("ALERT_LOG_PATH", "logs/alerts.log")


def _send_slack_webhook(formatted_message: str, webhook_url: str) -> None:
    """Post an alert message to a Slack or Discord incoming webhook URL."""
    response = requests.post(
        webhook_url,
        json={"text": formatted_message},
        timeout=5,
    )
    response.raise_for_status()


def _send_email_smtp(
    formatted_message: str,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    email_to: str,
) -> None:
    """Send an alert email over SMTP with STARTTLS."""
    msg = EmailMessage()
    msg["Subject"] = "[shop-dataops] Data Pipeline Alert"
    msg["From"] = smtp_user
    msg["To"] = email_to
    msg.set_content(formatted_message)

    with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.send_message(msg)


def send_alert(message: str, log_path: str | Path = DEFAULT_ALERT_LOG) -> str:
    """Write a timestamped alert to logs/alerts.log, print it, and notify Slack/Email if configured."""
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    formatted = f"[{timestamp}] ALERT: {message}"

    # Ensure the log directory exists before appending the alert line
    path = Path(log_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(formatted + "\n")

    print(formatted)

    # Real Slack/Webhook notification when ALERT_WEBHOOK_URL is configured via environment/secrets
    webhook_url = os.getenv("ALERT_WEBHOOK_URL", "").strip()
    if webhook_url:
        try:
            _send_slack_webhook(formatted, webhook_url)
        except Exception as exc:
            print(f"[{timestamp}] WARN: Failed to send webhook alert: {exc}")

    # Real SMTP email notification when ALERT_SMTP_HOST and ALERT_EMAIL_TO are configured
    smtp_host = os.getenv("ALERT_SMTP_HOST", "").strip()
    email_to = os.getenv("ALERT_EMAIL_TO", "").strip()
    if smtp_host and email_to:
        try:
            smtp_port = int(os.getenv("ALERT_SMTP_PORT", "587"))
            smtp_user = os.getenv("ALERT_SMTP_USER", "").strip()
            smtp_password = os.getenv("ALERT_SMTP_PASSWORD", "").strip()
            _send_email_smtp(
                formatted, smtp_host, smtp_port, smtp_user, smtp_password, email_to
            )
        except Exception as exc:
            print(f"[{timestamp}] WARN: Failed to send email alert: {exc}")

    return formatted

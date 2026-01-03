from __future__ import annotations

import os
import sys
from typing import Optional

import requests


def send_slack_message(
    text: str,
    *,
    webhook_env: str = "SLACK_WEBHOOK_URL",
    timeout: int = 10,
) -> Optional[int]:
    url = os.getenv(webhook_env)
    if not url:
        print("Slack: SLACK_WEBHOOK_URL not set; skipping", file=sys.stderr)
        return None

    payload = {"text": text}

    channel = os.getenv("SLACK_CHANNEL")
    username = os.getenv("SLACK_USERNAME")
    if channel:
        payload["channel"] = channel
    if username:
        payload["username"] = username

    try:
        r = requests.post(url, json=payload, timeout=timeout)
        if r.status_code >= 300:
            print(f"Slack: POST failed {r.status_code} {r.text}", file=sys.stderr)
        return r.status_code
    except Exception as e:
        print(f"Slack: exception sending message: {type(e).__name__}: {e}", file=sys.stderr)
        return None

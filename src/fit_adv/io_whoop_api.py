from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

WHOOP_TOKEN_URL = "https://api.prod.whoop.com/oauth/oauth2/token"
WHOOP_API_BASE = "https://api.prod.whoop.com/developer/v2"

DEFAULT_ENV_PATH = Path.home() / ".config" / "fit_adv" / "fit_adv.env"


@dataclass
class WhoopTokens:
    access_token: str
    refresh_token: Optional[str] = None
    expires_in: Optional[int] = None  # seconds
    obtained_at: float = field(default_factory=time.time)

    def is_expired(self, skew_seconds: int = 60) -> bool:
        if not self.expires_in:
            return False
        return (time.time() - self.obtained_at) >= (self.expires_in - skew_seconds)


class WhoopApiError(RuntimeError):
    pass


def _persist_refresh_token(new_refresh_token: str, env_path: Path = DEFAULT_ENV_PATH) -> None:
    """
    Update WHOOP_REFRESH_TOKEN in the env file so the next run uses the rotated token.
    """
    env_path.parent.mkdir(parents=True, exist_ok=True)

    lines = env_path.read_text().splitlines() if env_path.exists() else []

    key = "WHOOP_REFRESH_TOKEN"
    out: List[str] = []
    replaced = False

    for line in lines:
        if line.startswith(f"{key}="):
            out.append(f"{key}={new_refresh_token}")
            replaced = True
        else:
            out.append(line)

    if not replaced:
        out.append(f"{key}={new_refresh_token}")

    env_path.write_text("\n".join(out) + "\n")


def refresh_access_token(
    *,
    client_id: str,
    client_secret: str,
    refresh_token: str,
    scope: str = "offline",
    timeout: int = 30,
) -> WhoopTokens:
    """
    Refresh access token using WHOOP OAuth refresh flow (x-www-form-urlencoded POST).
    """
    client_id = client_id.strip()
    client_secret = client_secret.strip()
    refresh_token = refresh_token.strip()
    scope = scope.strip()

    data = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "scope": scope,
    }

    r = requests.post(
        WHOOP_TOKEN_URL,
        data=data,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        timeout=timeout,
    )
    if r.status_code != 200:
        raise WhoopApiError(f"Token refresh failed: {r.status_code} {r.text}")

    payload = r.json()

    # WHOOP may rotate refresh tokens; persist the new one if it changed.
    new_rt = payload.get("refresh_token")
    if new_rt:
        new_rt = str(new_rt).strip()
        if new_rt and new_rt != refresh_token:
            _persist_refresh_token(new_rt)

    return WhoopTokens(
        access_token=payload["access_token"],
        refresh_token=payload.get("refresh_token"),
        expires_in=payload.get("expires_in"),
        obtained_at=time.time(),
    )


def _request_with_backoff(
    method: str,
    url: str,
    *,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
    max_retries: int = 6,
) -> requests.Response:
    """
    Handles 429 rate limiting and transient 5xx errors via exponential backoff.
    """
    backoff = 1.0
    last_response: Optional[requests.Response] = None

    for _attempt in range(max_retries + 1):
        r = requests.request(method, url, headers=headers, params=params, timeout=timeout)
        last_response = r

        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            wait = float(retry_after) if retry_after else backoff
            time.sleep(wait)
            backoff = min(backoff * 2, 60.0)
            continue

        if 500 <= r.status_code <= 599:
            time.sleep(backoff)
            backoff = min(backoff * 2, 60.0)
            continue

        return r

    return last_response  # type: ignore[return-value]


def _get_auth_headers(access_token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }


def fetch_collection(
    *,
    access_token: str,
    path: str,
    since: Optional[str] = None,
    limit: int = 25,
) -> List[Dict[str, Any]]:
    """
    Fetch a WHOOP V2 "collection" endpoint with pagination via nextToken.

    since: ISO-8601 timestamp string used as the v2 'start' query param.
    """
    limit = max(1, min(int(limit), 25))

    url = f"{WHOOP_API_BASE}{path}"
    headers = _get_auth_headers(access_token)

    results: List[Dict[str, Any]] = []
    next_token: Optional[str] = None

    while True:
        params: Dict[str, Any] = {"limit": limit}
        if since:
            params["start"] = since
        if next_token:
            params["nextToken"] = next_token

        r = _request_with_backoff("GET", url, headers=headers, params=params)
        if r.status_code != 200:
            raise WhoopApiError(f"GET {path} failed: {r.status_code} {r.text}")

        payload = r.json()
        results.extend(payload.get("records", []))

        next_token = payload.get("nextToken") or payload.get("next_token") or ""
        if not next_token:
            break

    return results


def get_whoop_env() -> Tuple[str, str, str]:
    cid = os.getenv("WHOOP_CLIENT_ID")
    csecret = os.getenv("WHOOP_CLIENT_SECRET")
    rtoken = os.getenv("WHOOP_REFRESH_TOKEN")

    missing = [
        k
        for k, v in [
            ("WHOOP_CLIENT_ID", cid),
            ("WHOOP_CLIENT_SECRET", csecret),
            ("WHOOP_REFRESH_TOKEN", rtoken),
        ]
        if not v
    ]
    if missing:
        raise WhoopApiError(f"Missing env vars: {', '.join(missing)}")

    return cid.strip(), csecret.strip(), rtoken.strip()


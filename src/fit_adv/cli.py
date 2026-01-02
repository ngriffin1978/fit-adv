from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from fit_adv.pipeline import build_daily_from_latest_raw
from fit_adv.config import get_settings


def _cmd_build_daily() -> int:
    result = build_daily_from_latest_raw()
    print("Wrote:", result.outputs.daily_full_csv)
    print("Wrote:", result.outputs.daily_v1_csv)
    print("Rows:", result.df_daily.shape[0])
    return 0


def _cmd_whoop_pull(*, since: str | None, since_hours: int, limit: int) -> int:
    # Local imports so build-daily still works even if WHOOP deps evolve
    from fit_adv.io_raw_writer import write_raw_json
    from fit_adv.io_whoop_api import get_whoop_env, refresh_access_token, fetch_collection

    s = get_settings()

    # Default window if caller didn't provide an explicit since timestamp
    if since is None:
        since = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()

    # WHOOP v2 enforces limit <= 25; clamp here so CLI never trips it.
    limit = max(1, min(int(limit), 25))

    client_id, client_secret, refresh_token = get_whoop_env()
    tokens = refresh_access_token(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
    )

    # WHOOP Developer v2 collection endpoints
    cycles = fetch_collection(access_token=tokens.access_token, path="/cycle", since=since, limit=limit)
    recovery = fetch_collection(access_token=tokens.access_token, path="/recovery", since=since, limit=limit)
    sleeps = fetch_collection(access_token=tokens.access_token, path="/activity/sleep", since=since, limit=limit)
    workouts = fetch_collection(access_token=tokens.access_token, path="/activity/workout", since=since, limit=limit)

    p_cycle = write_raw_json(s.raw_dir, "cycle", cycles)
    p_recovery = write_raw_json(s.raw_dir, "recovery", recovery)
    p_sleep = write_raw_json(s.raw_dir, "sleep", sleeps)
    p_workout = write_raw_json(s.raw_dir, "workout", workouts)

    print("Wrote:", p_cycle)
    print("Wrote:", p_recovery)
    print("Wrote:", p_sleep)
    print("Wrote:", p_workout)
    print(
        "Counts:",
        {
            "cycle": len(cycles),
            "recovery": len(recovery),
            "sleep": len(sleeps),
            "workout": len(workouts),
        },
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="fit-adv")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("build-daily", help="Build daily_full.csv and daily_v1.csv from latest raw JSON")

    p_pull = sub.add_parser("whoop-pull", help="Pull WHOOP API data and write raw JSON files into data/raw")
    p_pull.add_argument(
        "--since",
        default=None,
        help="ISO-8601 timestamp (UTC recommended). If omitted, uses --since-hours window.",
    )
    p_pull.add_argument(
        "--since-hours",
        type=int,
        default=48,
        help="Pull data within the last N hours if --since is not provided (default: 48).",
    )
    p_pull.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Page size for WHOOP collection endpoints (max 25; default: 25).",
    )

    p_pull_build = sub.add_parser("whoop-pull-and-build", help="Pull WHOOP data then run build-daily")
    p_pull_build.add_argument("--since", default=None, help="ISO-8601 timestamp; overrides --since-hours.")
    p_pull_build.add_argument("--since-hours", type=int, default=48, help="Fallback window if --since not provided.")
    p_pull_build.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Page size for WHOOP collection endpoints (max 25; default: 25).",
    )

    args = parser.parse_args()

    if args.cmd == "build-daily":
        return _cmd_build_daily()

    if args.cmd == "whoop-pull":
        return _cmd_whoop_pull(since=args.since, since_hours=args.since_hours, limit=args.limit)

    if args.cmd == "whoop-pull-and-build":
        rc = _cmd_whoop_pull(since=args.since, since_hours=args.since_hours, limit=args.limit)
        if rc != 0:
            return rc
        return _cmd_build_daily()

    return 1


if __name__ == "__main__":
    raise SystemExit(main())


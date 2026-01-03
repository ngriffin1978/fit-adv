from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from fit_adv.pipeline import build_daily_from_latest_raw, build_daily_from_all_raw
from fit_adv.config import get_settings

from fit_adv.ops_context import RunContext
from fit_adv.ops_runlog import write_last_success
from fit_adv.ops_slack_format import format_run_message
from fit_adv.ops_slack_send import send_slack_message

from fit_adv.ops_metrics import write_run_metrics
from fit_adv.backfill import compute_backfill_range, iter_windows


def _run_with_slack(ctx: RunContext, fn) -> int:
    """
    Wrap a command with Slack success/failure notifications, last-success tracking,
    and JSON metrics emission.
    """
    try:
        rc = fn()
        ctx.end()

        # Write metrics (success)
        metrics_path = write_run_metrics(ctx, ok=True)
        ctx.add_output("metrics_json", str(metrics_path))

        send_slack_message(format_run_message(ctx, ok=True))
        write_last_success(
            ctx.service,
            {
                "duration_s": round(ctx.duration_s, 2),
                "endpoints": ctx.endpoints,
                "outputs": ctx.outputs,
            },
        )
        return int(rc)

    except Exception as e:
        ctx.end()
        err = f"{type(e).__name__}: {e}"

        # Write metrics (failure)
        metrics_path = write_run_metrics(ctx, ok=False, error=err)
        ctx.add_output("metrics_json", str(metrics_path))

        send_slack_message(format_run_message(ctx, ok=False, error=err))
        print(f"ERROR: {err}")
        return 1


def _cmd_build_daily(*, rebuild_from: str | None, rebuild_to: str | None, full_rebuild: bool) -> int:
    ctx = RunContext(service="build-daily", since=rebuild_from, since_hours=None, limit=None)

    def _impl() -> int:
        if full_rebuild or rebuild_from or rebuild_to:
            # Historical rebuild from *all* raw
            result = build_daily_from_all_raw(start=rebuild_from, end=rebuild_to)
            ctx.extra["mode"] = "rebuild"
            ctx.extra["rebuild_from"] = rebuild_from
            ctx.extra["rebuild_to"] = rebuild_to
            ctx.extra["full_rebuild"] = bool(full_rebuild)
        else:
            # Normal daily run from *latest* raw
            result = build_daily_from_latest_raw()
            ctx.extra["mode"] = "latest"

        ctx.add_output("daily_full_csv", str(result.outputs.daily_full_csv))
        ctx.add_output("daily_v1_csv", str(result.outputs.daily_v1_csv))
        ctx.extra["rows"] = int(result.df_daily.shape[0])

        print("Wrote:", result.outputs.daily_full_csv)
        print("Wrote:", result.outputs.daily_v1_csv)
        print("Rows:", result.df_daily.shape[0])
        return 0

    return _run_with_slack(ctx, _impl)


def _cmd_whoop_pull(*, since: str | None, since_hours: int, limit: int, service_name: str = "whoop-pull") -> int:
    # Local imports so build-daily still works even if WHOOP deps evolve
    from fit_adv.io_raw_writer import write_raw_json
    from fit_adv.io_whoop_api import get_whoop_env, refresh_access_token, fetch_collection

    ctx = RunContext(service=service_name, since=since, since_hours=since_hours, limit=limit)

    def _impl() -> int:
        s = get_settings()

        # Default window if caller didn't provide an explicit since timestamp
        if ctx.since is None:
            ctx.since = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()

        # WHOOP v2 enforces limit <= 25; clamp here so CLI never trips it.
        ctx.limit = max(1, min(int(limit), 25))

        client_id, client_secret, refresh_token = get_whoop_env()
        tokens = refresh_access_token(
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
        )

        # WHOOP Developer v2 collection endpoints
        cycles = fetch_collection(
            access_token=tokens.access_token, path="/cycle", since=ctx.since, limit=ctx.limit
        )
        recovery = fetch_collection(
            access_token=tokens.access_token, path="/recovery", since=ctx.since, limit=ctx.limit
        )
        sleeps = fetch_collection(
            access_token=tokens.access_token, path="/activity/sleep", since=ctx.since, limit=ctx.limit
        )
        workouts = fetch_collection(
            access_token=tokens.access_token, path="/activity/workout", since=ctx.since, limit=ctx.limit
        )

        # Endpoint metrics (pages omitted for now; easy to add later)
        ctx.add_endpoint("/cycle", records=len(cycles))
        ctx.add_endpoint("/recovery", records=len(recovery))
        ctx.add_endpoint("/activity/sleep", records=len(sleeps))
        ctx.add_endpoint("/activity/workout", records=len(workouts))

        p_cycle = write_raw_json(s.raw_dir, "cycle", cycles)
        p_recovery = write_raw_json(s.raw_dir, "recovery", recovery)
        p_sleep = write_raw_json(s.raw_dir, "sleep", sleeps)
        p_workout = write_raw_json(s.raw_dir, "workout", workouts)

        ctx.add_output("raw_cycle", str(p_cycle))
        ctx.add_output("raw_recovery", str(p_recovery))
        ctx.add_output("raw_sleep", str(p_sleep))
        ctx.add_output("raw_workout", str(p_workout))

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

    return _run_with_slack(ctx, _impl)


def _cmd_whoop_pull_and_build(*, since: str | None, since_hours: int, limit: int) -> int:
    ctx = RunContext(service="whoop-pull-and-build", since=since, since_hours=since_hours, limit=limit)

    def _impl() -> int:
        from fit_adv.io_raw_writer import write_raw_json
        from fit_adv.io_whoop_api import get_whoop_env, refresh_access_token, fetch_collection

        s = get_settings()

        if ctx.since is None:
            ctx.since = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()

        ctx.limit = max(1, min(int(limit), 25))

        client_id, client_secret, refresh_token = get_whoop_env()
        tokens = refresh_access_token(
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
        )

        cycles = fetch_collection(access_token=tokens.access_token, path="/cycle", since=ctx.since, limit=ctx.limit)
        recovery = fetch_collection(access_token=tokens.access_token, path="/recovery", since=ctx.since, limit=ctx.limit)
        sleeps = fetch_collection(access_token=tokens.access_token, path="/activity/sleep", since=ctx.since, limit=ctx.limit)
        workouts = fetch_collection(access_token=tokens.access_token, path="/activity/workout", since=ctx.since, limit=ctx.limit)

        ctx.add_endpoint("/cycle", records=len(cycles))
        ctx.add_endpoint("/recovery", records=len(recovery))
        ctx.add_endpoint("/activity/sleep", records=len(sleeps))
        ctx.add_endpoint("/activity/workout", records=len(workouts))

        p_cycle = write_raw_json(s.raw_dir, "cycle", cycles)
        p_recovery = write_raw_json(s.raw_dir, "recovery", recovery)
        p_sleep = write_raw_json(s.raw_dir, "sleep", sleeps)
        p_workout = write_raw_json(s.raw_dir, "workout", workouts)

        ctx.add_output("raw_cycle", str(p_cycle))
        ctx.add_output("raw_recovery", str(p_recovery))
        ctx.add_output("raw_sleep", str(p_sleep))
        ctx.add_output("raw_workout", str(p_workout))

        # Build daily (latest) — this is the intended behavior for pull-and-build
        result = build_daily_from_latest_raw()
        ctx.add_output("daily_full_csv", str(result.outputs.daily_full_csv))
        ctx.add_output("daily_v1_csv", str(result.outputs.daily_v1_csv))
        ctx.extra["rows"] = int(result.df_daily.shape[0])

        print("Wrote:", p_cycle)
        print("Wrote:", p_recovery)
        print("Wrote:", p_sleep)
        print("Wrote:", p_workout)
        print("Wrote:", result.outputs.daily_full_csv)
        print("Wrote:", result.outputs.daily_v1_csv)
        print("Rows:", result.df_daily.shape[0])

        return 0

    return _run_with_slack(ctx, _impl)


def _cmd_whoop_backfill(
    *,
    since: str | None,
    days: int | None,
    until: str | None,
    chunk_hours: int,
    limit: int,
    build_daily: bool,
) -> int:
    """
    Backfill WHOOP raw data over a historical range in chunked windows.
    Writes per-window metrics JSON plus one overall run metrics JSON + Slack.
    """
    from fit_adv.io_raw_writer import write_raw_json
    from fit_adv.io_whoop_api import get_whoop_env, refresh_access_token, fetch_collection

    ctx = RunContext(service="whoop-backfill", since=since, since_hours=None, limit=limit)

    def _impl() -> int:
        s = get_settings()

        ctx.limit = max(1, min(int(limit), 25))
        start_dt, end_dt = compute_backfill_range(since=since, days=days, until=until)

        ctx.extra["range_start"] = start_dt.isoformat()
        ctx.extra["range_end"] = end_dt.isoformat()
        ctx.extra["chunk_hours"] = int(chunk_hours)

        windows = list(iter_windows(start_dt, end_dt, chunk_hours=int(chunk_hours)))
        ctx.extra["windows_total"] = len(windows)

        client_id, client_secret, refresh_token = get_whoop_env()
        tokens = refresh_access_token(
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
        )

        total_counts = {"cycle": 0, "recovery": 0, "sleep": 0, "workout": 0}
        windows_ok = 0

        for idx, w in enumerate(windows, start=1):
            wctx = RunContext(service="whoop-backfill-window", since=w.start_iso, since_hours=None, limit=ctx.limit)
            wctx.extra["window_index"] = idx
            wctx.extra["windows_total"] = len(windows)
            wctx.extra["window_end"] = w.end_iso

            try:
                # IMPORTANT: bound each window with BOTH start and end
                cycles = fetch_collection(
                    access_token=tokens.access_token,
                    path="/cycle",
                    since=w.start_iso,
                    until=w.end_iso,
                    limit=ctx.limit,
                )
                recovery = fetch_collection(
                    access_token=tokens.access_token,
                    path="/recovery",
                    since=w.start_iso,
                    until=w.end_iso,
                    limit=ctx.limit,
                )
                sleeps = fetch_collection(
                    access_token=tokens.access_token,
                    path="/activity/sleep",
                    since=w.start_iso,
                    until=w.end_iso,
                    limit=ctx.limit,
                )
                workouts = fetch_collection(
                    access_token=tokens.access_token,
                    path="/activity/workout",
                    since=w.start_iso,
                    until=w.end_iso,
                    limit=ctx.limit,
                )

                wctx.add_endpoint("/cycle", records=len(cycles))
                wctx.add_endpoint("/recovery", records=len(recovery))
                wctx.add_endpoint("/activity/sleep", records=len(sleeps))
                wctx.add_endpoint("/activity/workout", records=len(workouts))

                p_cycle = write_raw_json(s.raw_dir, "cycle", cycles)
                p_recovery = write_raw_json(s.raw_dir, "recovery", recovery)
                p_sleep = write_raw_json(s.raw_dir, "sleep", sleeps)
                p_workout = write_raw_json(s.raw_dir, "workout", workouts)

                wctx.add_output("raw_cycle", str(p_cycle))
                wctx.add_output("raw_recovery", str(p_recovery))
                wctx.add_output("raw_sleep", str(p_sleep))
                wctx.add_output("raw_workout", str(p_workout))

                total_counts["cycle"] += len(cycles)
                total_counts["recovery"] += len(recovery)
                total_counts["sleep"] += len(sleeps)
                total_counts["workout"] += len(workouts)
                windows_ok += 1

                wctx.end()
                write_run_metrics(wctx, ok=True)

                print(f"[{idx}/{len(windows)}] OK window {w.start_iso} -> {w.end_iso}")

            except Exception as e:
                wctx.end()
                err = f"{type(e).__name__}: {e}"
                write_run_metrics(wctx, ok=False, error=err)

                ctx.extra["failed_window_index"] = idx
                ctx.extra["failed_window_start"] = w.start_iso
                ctx.extra["failed_window_end"] = w.end_iso
                raise

        ctx.extra["windows_ok"] = windows_ok
        ctx.extra["totals"] = total_counts

        ctx.add_endpoint("/cycle", records=total_counts["cycle"])
        ctx.add_endpoint("/recovery", records=total_counts["recovery"])
        ctx.add_endpoint("/activity/sleep", records=total_counts["sleep"])
        ctx.add_endpoint("/activity/workout", records=total_counts["workout"])

        if build_daily:
            # After backfill, rebuild from ALL raw so historical windows are reflected.
            result = build_daily_from_all_raw(start=ctx.extra.get("range_start"), end=ctx.extra.get("range_end"))
            ctx.add_output("daily_full_csv", str(result.outputs.daily_full_csv))
            ctx.add_output("daily_v1_csv", str(result.outputs.daily_v1_csv))
            ctx.extra["rows"] = int(result.df_daily.shape[0])

        return 0

    return _run_with_slack(ctx, _impl)


def main() -> int:
    parser = argparse.ArgumentParser(prog="fit-adv")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_build = sub.add_parser("build-daily", help="Build daily_full.csv and daily_v1.csv from raw JSON")
    p_build.add_argument("--rebuild-from", dest="rebuild_from", default=None, help="Rebuild starting at YYYY-MM-DD or ISO timestamp (UTC).")
    p_build.add_argument("--rebuild-to", dest="rebuild_to", default=None, help="Rebuild up to (exclusive) YYYY-MM-DD or ISO timestamp (UTC).")
    p_build.add_argument("--full-rebuild", dest="full_rebuild", action="store_true", help="Rebuild from all raw JSON (ignores latest-only mode).")

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

    p_backfill = sub.add_parser("whoop-backfill", help="Backfill WHOOP raw data over a historical range")
    p_backfill.add_argument("--since", default=None, help="ISO timestamp or YYYY-MM-DD (UTC). Overrides --days.")
    p_backfill.add_argument("--days", type=int, default=30, help="Backfill last N days if --since not provided (default: 30).")
    p_backfill.add_argument("--until", default=None, help="ISO timestamp or YYYY-MM-DD (UTC). Defaults to now (UTC).")
    p_backfill.add_argument("--chunk-hours", type=int, default=24, help="Window size in hours (default: 24).")
    p_backfill.add_argument("--limit", type=int, default=25, help="WHOOP page size (max 25; default: 25).")
    p_backfill.add_argument("--build-daily", action="store_true", help="Run build-daily once after backfill completes (rebuilds from all raw).")

    args = parser.parse_args()

    if args.cmd == "build-daily":
        return _cmd_build_daily(
            rebuild_from=getattr(args, "rebuild_from", None),
            rebuild_to=getattr(args, "rebuild_to", None),
            full_rebuild=bool(getattr(args, "full_rebuild", False)),
        )

    if args.cmd == "whoop-pull":
        return _cmd_whoop_pull(since=args.since, since_hours=args.since_hours, limit=args.limit)

    if args.cmd == "whoop-pull-and-build":
        return _cmd_whoop_pull_and_build(since=args.since, since_hours=args.since_hours, limit=args.limit)

    if args.cmd == "whoop-backfill":
        return _cmd_whoop_backfill(
            since=args.since,
            days=args.days,
            until=args.until,
            chunk_hours=args.chunk_hours,
            limit=args.limit,
            build_daily=args.build_daily,
        )

    return 1


if __name__ == "__main__":
    raise SystemExit(main())

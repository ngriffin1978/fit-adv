from __future__ import annotations

from typing import Optional

from fit_adv.ops_context import RunContext
from fit_adv.ops_runlog import read_last_success


def _slack_rel_time(unix_ts: int) -> str:
    # Slack: <t:UNIX:R> renders as "2 hours ago"
    return f"<t:{unix_ts}:R>"


def format_run_message(ctx: RunContext, *, ok: bool, error: Optional[str] = None) -> str:
    status = "✅ SUCCESS" if ok else "❌ FAILURE"
    dur = f"{ctx.duration_s:.1f}s"

    lines: list[str] = []
    lines.append(f"*fit-adv* — *{ctx.service}* — {status}")
    lines.append(f"Duration: `{dur}`")

    if ctx.since_hours is not None:
        lines.append(f"since_hours: `{ctx.since_hours}`")
    if ctx.since is not None:
        lines.append(f"since: `{ctx.since}`")
    if ctx.limit is not None:
        lines.append(f"limit: `{ctx.limit}`")

    if ctx.endpoints:
        lines.append("\n*Endpoints:*")
        for path, m in ctx.endpoints.items():
            rec = m.get("records", 0)
            pages = m.get("pages", 0)
            if pages:
                lines.append(f"• `{path}` — records: `{rec}` pages: `{pages}`")
            else:
                lines.append(f"• `{path}` — records: `{rec}`")

    if ctx.outputs:
        lines.append("\n*Outputs:*")
        for k, v in ctx.outputs.items():
            lines.append(f"• `{k}`: `{v}`")

    if error:
        lines.append("\n*Error:*")
        # keep it short; full details are in journalctl
        lines.append(f"```{error[:900]}```")

    last = read_last_success()
    if last and isinstance(last.get("ts"), int):
        lines.append(f"\nLast success: {_slack_rel_time(last['ts'])} ({last.get('service','')})")

    return "\n".join(lines)

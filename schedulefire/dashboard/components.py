"""Small HTML building blocks shared by the dashboard pages. All dynamic text goes
through ``h()`` (HTML escaping)."""

from __future__ import annotations

import html
import json
import time
from datetime import datetime, timezone
from typing import Iterable, List, Optional

from ..cron import Cron
from ..states import AWAITING, DELETED, ENQUEUED, FAILED, PROCESSING, SCHEDULED, SUCCEEDED
from .assets import icon

h = html.escape

STATE_META = {
    ENQUEUED: ("enqueued", "Jobs waiting in a queue for a free worker."),
    SCHEDULED: ("scheduled", "Delayed jobs and retries waiting for their time to come."),
    PROCESSING: ("processing", "Jobs being performed by a worker right now."),
    SUCCEEDED: ("succeeded", "Jobs that completed. They are removed automatically after the expiration period."),
    FAILED: ("failed", "Jobs that failed and exhausted their retries. Failed jobs are kept until you act on them."),
    DELETED: ("deleted", "Jobs that were deleted manually or after exceeding retry attempts."),
    AWAITING: ("awaiting", "Continuations waiting for their parent job to finish."),
}


def state_dot(state: Optional[str]) -> str:
    key = STATE_META.get(state or "", ("deleted",))[0]
    return f'<span class="dot" style="background:var(--s-{key})"></span>'


def pill(state: Optional[str], large: bool = False) -> str:
    label = state or "Created"
    return f'<span class="pill{" lg" if large else ""}">{state_dot(state)}{h(label)}</span>'


def absolute(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def relative_text(ts: float, now: Optional[float] = None) -> str:
    delta = ts - (now or time.time())
    seconds = abs(delta)
    if seconds < 45:
        return "just now" if delta <= 0 else "in a few seconds"
    for unit, size in (("d", 86400), ("h", 3600), ("min", 60)):
        if seconds >= size:
            n = int(seconds // size)
            label = f"{n} {unit}"
            break
    else:
        label = f"{int(seconds)} s"
    return f"in {label}" if delta > 0 else f"{label} ago"


def when(ts: Optional[float], placeholder: str = "—") -> str:
    if ts is None:
        return f'<span class="muted">{placeholder}</span>'
    return f'<time class="nowrap" datetime="{h(datetime.fromtimestamp(ts, timezone.utc).isoformat())}" ' \
           f'title="{absolute(ts)}">{relative_text(ts)}</time>'


def parse_iso(value) -> Optional[float]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).timestamp()
    except (TypeError, ValueError):
        return None


def compact(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}".rstrip("0").rstrip(".") + "M"
    if n >= 10_000:
        return f"{n / 1000:.1f}".rstrip("0").rstrip(".") + "K"
    return f"{n:,}"


def duration(ms) -> str:
    if ms is None:
        return '<span class="muted">—</span>'
    if ms < 1000:
        return f"{ms} ms"
    seconds = ms / 1000
    if seconds < 60:
        return f"{seconds:.2f} s"
    minutes, seconds = divmod(int(seconds), 60)
    if minutes < 60:
        return f"{minutes}m {seconds}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m"


def span_text(seconds: float) -> str:
    return duration(int(seconds * 1000))


def empty_state(icon_name: str, title: str, text: str = "") -> str:
    return (f'<div class="empty"><div class="ring">{icon(icon_name)}</div><h3>{h(title)}</h3>'
            f'{f"<p>{text}</p>" if text else ""}</div>')


def pretty_json(value) -> str:
    return h(json.dumps(value, indent=2, default=str, ensure_ascii=False))


DAYS_OF_WEEK = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
_MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September",
           "October", "November", "December"]
_MACROS = {"@yearly": "0 0 1 1 *", "@annually": "0 0 1 1 *", "@monthly": "0 0 1 * *", "@weekly": "0 0 * * 0",
           "@daily": "0 0 * * *", "@midnight": "0 0 * * *", "@hourly": "0 * * * *"}


def describe_cron(expr: str) -> Optional[str]:
    """Plain-English description for common schedules; None when it's not a simple one."""
    expr = expr.strip()
    if expr == Cron.never():
        return "Never (manual trigger only)"
    parts = _MACROS.get(expr.lower(), expr).split()
    if len(parts) != 5:
        return None
    m, hr, dom, mon, dow = parts
    rest_any = dom == mon == dow == "*"
    if parts == ["*"] * 5:
        return "Every minute"
    if m.startswith("*/") and m[2:].isdigit() and hr == "*" and rest_any:
        return f"Every {m[2:]} minutes"
    if not m.isdigit():
        return None
    if hr == "*" and rest_any:
        return f"Every hour at :{int(m):02d}"
    if hr.startswith("*/") and hr[2:].isdigit() and rest_any:
        return f"Every {hr[2:]} hours at :{int(m):02d}"
    if not hr.isdigit():
        return None
    at = f"{int(hr):02d}:{int(m):02d}"
    if rest_any:
        return f"Every day at {at}"
    if dom == mon == "*":
        if dow == "1-5":
            return f"Weekdays at {at}"
        if dow in ("0,6", "6,0"):
            return f"Weekends at {at}"
        if dow.isdigit() and int(dow) <= 7:
            return f"Every {DAYS_OF_WEEK[int(dow) % 7]} at {at}"
        return None
    if dow == "*" and dom.isdigit():
        if mon == "*":
            return f"Monthly on day {int(dom)} at {at}"
        if mon.isdigit() and 1 <= int(mon) <= 12:
            return f"Every year on {_MONTHS[int(mon) - 1]} {int(dom)} at {at}"
    return None


#: A curated set of common IANA zones, offered as suggestions (not a hard-coded allowlist -
#: any valid zone name still works) in the recurring-job schedule builder.
COMMON_TIME_ZONES = [
    "UTC", "America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles",
    "America/Sao_Paulo", "America/Mexico_City", "America/Toronto",
    "Europe/London", "Europe/Paris", "Europe/Berlin", "Europe/Madrid", "Europe/Moscow",
    "Africa/Cairo", "Africa/Johannesburg", "Africa/Lagos",
    "Asia/Dubai", "Asia/Kolkata", "Asia/Bangkok", "Asia/Shanghai", "Asia/Tokyo",
    "Asia/Seoul", "Asia/Singapore", "Asia/Hong_Kong",
    "Australia/Sydney", "Australia/Perth", "Pacific/Auckland",
]


def schedule_builder_state(expr: str) -> dict:
    """Break a cron expression down for the friendly schedule builder: which frequency
    (minutes/hourly/daily/weekly/monthly) it matches and that frequency's field values, all
    as strings ready to drop into HTML. Anything that doesn't fit a simple pattern - complex
    field combinations, ranges like weekday ranges, `Cron.never()` - falls back to
    ``{"frequency": "custom", "cron": expr}``, editable as a raw cron expression."""
    expr = expr.strip()
    fallback = {"frequency": "custom", "cron": expr}
    parts = _MACROS.get(expr.lower(), expr).split()
    if len(parts) != 5:
        return fallback
    m, hr, dom, mon, dow = parts
    rest_any = dom == mon == dow == "*"
    if m.startswith("*/") and m[2:].isdigit() and hr == "*" and rest_any:
        return {"frequency": "minutes", "interval_minutes": m[2:]}
    if not m.isdigit():
        return fallback
    if hr == "*" and rest_any:
        return {"frequency": "hourly", "interval_hours": "1", "minute": str(int(m))}
    if hr.startswith("*/") and hr[2:].isdigit() and rest_any:
        return {"frequency": "hourly", "interval_hours": hr[2:], "minute": str(int(m))}
    if not hr.isdigit():
        return fallback
    time_str = f"{int(hr):02d}:{int(m):02d}"
    if rest_any:
        return {"frequency": "daily", "time": time_str}
    if dom == mon == "*" and dow.isdigit() and int(dow) <= 7:
        return {"frequency": "weekly", "day_of_week": str(int(dow) % 7), "time": time_str}
    if dow == "*" and mon == "*" and dom.isdigit():
        return {"frequency": "monthly", "day_of_month": str(int(dom)), "time": time_str}
    return fallback


def throughput_chart(points: List[dict], label: str) -> str:
    """Legend + stacked columns (succeeded / failed). Drawn client-side to fit the width."""
    data = json.dumps(points, separators=(",", ":"))
    total_s = sum(p["s"] for p in points)
    total_f = sum(p["f"] for p in points)
    return (
        '<div class="legend">'
        f'<span><i style="background:var(--series-succeeded)"></i>Succeeded <b>{total_s:,}</b></span>'
        f'<span><i style="background:var(--series-failed)"></i>Failed <b>{total_f:,}</b></span></div>'
        f'<div class="chart" data-points="{h(data)}" aria-label="{h(label)}"></div>'
    )


def chart_table(points: List[dict]) -> str:
    """The chart's data as a table, so no value depends on hovering or on script."""
    rows = "".join(
        f'<tr><td>{h(p["label"])}</td><td class="r num">{p["s"]:,}</td><td class="r num">{p["f"]:,}</td></tr>'
        for p in reversed(points)
    )
    return (
        '<details class="table-view"><summary>View as table</summary><div class="table-wrap" style="max-height:280px">'
        '<table class="t"><thead><tr><th>Period</th><th class="r">Succeeded</th><th class="r">Failed</th></tr></thead>'
        f'<tbody>{rows}</tbody></table></div></details>'
    )


def job_cell(url: str, job, job_id: str) -> str:
    args = job.display_name(160)[len(job.qualname):]
    return (f'<a class="job" href="{h(url)}" title="{h(job.method_name)}">'
            f'<span class="fn">{h(job.qualname)}</span><span class="args mono">{h(args)}</span></a>')


def join(parts: Iterable[str]) -> str:
    return "".join(parts)

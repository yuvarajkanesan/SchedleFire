"""Cron expressions for recurring jobs.

Standard 5-field syntax ``minute hour day-of-month month day-of-week`` with ``*``,
``?``, lists (``1,15``), ranges (``1-5``), steps (``*/15``, ``10-40/5``), month and
weekday names (``jan``, ``mon-fri``) and macros (``@daily``, ``@hourly`` ...).
When both day-of-month and day-of-week are restricted, a day matches if *either*
matches (Vixie cron behaviour).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone, tzinfo
from typing import Dict, Optional, Set, Union

_MONTHS = {n: i for i, n in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], 1)}
_DAYS = {n: i for i, n in enumerate(["sun", "mon", "tue", "wed", "thu", "fri", "sat"])}

_MACROS = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}

# How far ahead to search before concluding an expression never fires (e.g. Feb 31).
_MAX_YEARS_AHEAD = 5


def _parse_field(text: str, lo: int, hi: int, names: Optional[Dict[str, int]] = None) -> Set[int]:
    def value(token: str) -> int:
        token = token.strip().lower()
        if names and token in names:
            return names[token]
        if not token.isdigit():
            raise ValueError(f"Invalid cron value {token!r}")
        return int(token)

    values: Set[int] = set()
    for part in text.split(","):
        step = 1
        has_step = "/" in part
        if has_step:
            part, step_text = part.split("/", 1)
            if not step_text.isdigit() or int(step_text) == 0:
                raise ValueError(f"Invalid cron step {step_text!r}")
            step = int(step_text)
        if part in ("*", "?"):
            start, end = lo, hi
        elif "-" in part:
            a, b = part.split("-", 1)
            start, end = value(a), value(b)
        else:
            start = value(part)
            end = hi if has_step else start
        if not (lo <= start <= hi and lo <= end <= hi and start <= end):
            raise ValueError(f"Cron field {text!r} is out of range {lo}-{hi}")
        values.update(range(start, end + 1, step))
    return values


class CronExpression:
    def __init__(self, expression: str):
        self.expression = expression.strip()
        text = _MACROS.get(self.expression.lower(), self.expression)
        parts = text.split()
        if len(parts) != 5:
            raise ValueError(
                f"Invalid cron expression {expression!r}: expected 5 fields "
                "'minute hour day-of-month month day-of-week'."
            )
        try:
            self.minutes = _parse_field(parts[0], 0, 59)
            self.hours = _parse_field(parts[1], 0, 23)
            self.days = _parse_field(parts[2], 1, 31)
            self.months = _parse_field(parts[3], 1, 12, _MONTHS)
            weekdays = _parse_field(parts[4], 0, 7, _DAYS)
        except ValueError as e:
            raise ValueError(f"Invalid cron expression {expression!r}: {e}") from None
        if 7 in weekdays:  # both 0 and 7 mean Sunday
            weekdays.discard(7)
            weekdays.add(0)
        self.weekdays = weekdays
        self._any_day = parts[2] in ("*", "?")
        self._any_weekday = parts[4] in ("*", "?")

    def _day_matches(self, d: datetime) -> bool:
        dom = d.day in self.days
        dow = (d.isoweekday() % 7) in self.weekdays
        if self._any_day and self._any_weekday:
            return True
        if self._any_day:
            return dow
        if self._any_weekday:
            return dom
        return dom or dow

    def _next_wall_time(self, after: datetime) -> Optional[datetime]:
        """Next matching naive wall-clock time strictly after ``after`` (naive)."""
        t = after.replace(second=0, microsecond=0) + timedelta(minutes=1)
        limit = after.year + _MAX_YEARS_AHEAD
        while t.year <= limit:
            if t.month not in self.months:
                t = (t.replace(day=1, hour=0, minute=0) + timedelta(days=32)).replace(day=1)
                continue
            if not self._day_matches(t):
                t = t.replace(hour=0, minute=0) + timedelta(days=1)
                continue
            if t.hour not in self.hours:
                t = t.replace(minute=0) + timedelta(hours=1)
                continue
            if t.minute not in self.minutes:
                t += timedelta(minutes=1)
                continue
            return t
        return None

    def next_occurrence(self, after: datetime, tz: Optional[tzinfo] = None) -> Optional[datetime]:
        """Next fire time after ``after`` (aware), evaluated in ``tz``; returned in UTC.

        Returns ``None`` when the expression never fires (e.g. ``Cron.never()``).
        """
        if after.tzinfo is None:
            raise ValueError("'after' must be timezone-aware")
        tz = tz or timezone.utc
        wall = after.astimezone(tz).replace(tzinfo=None)
        for _ in range(3):  # skip wall times that map to the past around DST changes
            wall = self._next_wall_time(wall)
            if wall is None:
                return None
            result = wall.replace(tzinfo=tz).astimezone(timezone.utc)
            if result > after:
                return result
        return result

    def __str__(self) -> str:
        return self.expression

    def __repr__(self) -> str:
        return f"CronExpression({self.expression!r})"


def _dow(day: Union[int, str]) -> str:
    if isinstance(day, str):
        if day.lower()[:3] not in _DAYS:
            raise ValueError(f"Unknown day of week {day!r}")
        return str(_DAYS[day.lower()[:3]])
    if not 0 <= day <= 6:
        raise ValueError("day_of_week must be 0 (Sunday) .. 6 (Saturday)")
    return str(day)


class Cron:
    """Helpers that build cron strings, mirroring Hangfire's ``Cron`` class."""

    @staticmethod
    def minutely() -> str:
        return "* * * * *"

    @staticmethod
    def hourly(minute: int = 0) -> str:
        return f"{minute} * * * *"

    @staticmethod
    def daily(hour: int = 0, minute: int = 0) -> str:
        return f"{minute} {hour} * * *"

    @staticmethod
    def weekly(day_of_week: Union[int, str] = 1, hour: int = 0, minute: int = 0) -> str:
        """``day_of_week``: 0=Sunday .. 6=Saturday, or a name like ``"fri"``. Default Monday."""
        return f"{minute} {hour} * * {_dow(day_of_week)}"

    @staticmethod
    def monthly(day: int = 1, hour: int = 0, minute: int = 0) -> str:
        return f"{minute} {hour} {day} * *"

    @staticmethod
    def yearly(month: int = 1, day: int = 1, hour: int = 0, minute: int = 0) -> str:
        return f"{minute} {hour} {day} {month} *"

    @staticmethod
    def never() -> str:
        """A schedule that never fires; the job can still be run with ``RecurringJob.trigger``."""
        return "0 0 31 2 *"

    @staticmethod
    def minute_interval(interval: int) -> str:
        return f"*/{interval} * * * *"

    @staticmethod
    def hour_interval(interval: int) -> str:
        return f"0 */{interval} * * *"

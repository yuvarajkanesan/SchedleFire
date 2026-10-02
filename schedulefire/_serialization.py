"""JSON serialization for job arguments, parameters and results.

Plain JSON types round-trip as-is. A few common non-JSON types (datetime, date,
time, timedelta, Decimal, UUID, bytes, set) are encoded as tagged objects so they
come back with the same type on the worker. Tuples come back as lists.
"""

from __future__ import annotations

import base64
import datetime as dt
import decimal
import json
import uuid
from typing import Any

_TYPE = "__schedulefire_type__"


def _encode(o: Any) -> Any:
    if isinstance(o, dt.datetime):
        return {_TYPE: "datetime", "v": o.isoformat()}
    if isinstance(o, dt.date):
        return {_TYPE: "date", "v": o.isoformat()}
    if isinstance(o, dt.time):
        return {_TYPE: "time", "v": o.isoformat()}
    if isinstance(o, dt.timedelta):
        return {_TYPE: "timedelta", "v": o.total_seconds()}
    if isinstance(o, decimal.Decimal):
        return {_TYPE: "decimal", "v": str(o)}
    if isinstance(o, uuid.UUID):
        return {_TYPE: "uuid", "v": str(o)}
    if isinstance(o, (bytes, bytearray)):
        return {_TYPE: "bytes", "v": base64.b64encode(bytes(o)).decode("ascii")}
    if isinstance(o, (set, frozenset)):
        return {_TYPE: "set", "v": list(o)}
    raise TypeError(
        f"Object of type {type(o).__name__} is not serializable. Job arguments must be "
        "JSON-compatible values (str, int, float, bool, None, list, dict) or one of: "
        "datetime, date, time, timedelta, Decimal, UUID, bytes, set."
    )


_DECODERS = {
    "datetime": dt.datetime.fromisoformat,
    "date": dt.date.fromisoformat,
    "time": dt.time.fromisoformat,
    "timedelta": lambda v: dt.timedelta(seconds=v),
    "decimal": decimal.Decimal,
    "uuid": uuid.UUID,
    "bytes": lambda v: base64.b64decode(v),
    "set": set,
}


def _decode(d: dict) -> Any:
    kind = d.get(_TYPE)
    if kind is None or len(d) != 2 or kind not in _DECODERS:
        return d
    return _DECODERS[kind](d["v"])


def dumps(obj: Any) -> str:
    return json.dumps(obj, default=_encode, separators=(",", ":"), ensure_ascii=False)


def loads(text: str) -> Any:
    return json.loads(text, object_hook=_decode)


def to_safe_value(obj: Any, max_repr: int = 2000) -> Any:
    """Return ``obj`` if it serializes, otherwise a truncated ``repr`` (used for job results)."""
    try:
        dumps(obj)
        return obj
    except (TypeError, ValueError):
        text = repr(obj)
        return text if len(text) <= max_repr else text[:max_repr] + "..."

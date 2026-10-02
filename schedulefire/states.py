"""Job states - the same state model as Hangfire.

::

    Scheduled ──► Enqueued ──► Processing ──► Succeeded
       ▲             ▲             │
       └── (retry) ──┼─────────────┴──► Failed
    Awaiting ────────┘       any state ──► Deleted
"""

from __future__ import annotations

import time
import traceback
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import ClassVar, Optional, Union

from . import _serialization

ENQUEUED = "Enqueued"
SCHEDULED = "Scheduled"
PROCESSING = "Processing"
SUCCEEDED = "Succeeded"
FAILED = "Failed"
DELETED = "Deleted"
AWAITING = "Awaiting"

ALL_STATES = (ENQUEUED, SCHEDULED, PROCESSING, SUCCEEDED, FAILED, DELETED, AWAITING)


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat()


def to_timestamp(when: Union[datetime, timedelta, int, float]) -> float:
    """Normalize an absolute time or a delay (timedelta / seconds) into a UTC epoch timestamp."""
    if isinstance(when, datetime):
        if when.tzinfo is None:
            raise ValueError("Use a timezone-aware datetime (e.g. datetime.now(timezone.utc)).")
        return when.timestamp()
    if isinstance(when, timedelta):
        return time.time() + when.total_seconds()
    if isinstance(when, (int, float)) and not isinstance(when, bool):
        return time.time() + float(when)
    raise TypeError(f"Expected datetime, timedelta or seconds, got {type(when).__name__}.")


class ContinuationOptions(str, Enum):
    ONLY_ON_SUCCEEDED_STATE = "OnlyOnSucceededState"
    ON_ANY_FINISHED_STATE = "OnAnyFinishedState"


class State:
    name: ClassVar[str] = ""
    #: Final states get an expiration time and are cleaned up automatically.
    is_final: ClassVar[bool] = False

    def __init__(self, reason: Optional[str] = None):
        self.reason = reason

    def serialize(self) -> dict:
        return {}

    def __repr__(self) -> str:
        return f"<{type(self).__name__} reason={self.reason!r}>"


class EnqueuedState(State):
    """Job is waiting in a queue. ``queue=None`` means the job's own queue."""

    name = ENQUEUED

    def __init__(self, queue: Optional[str] = None, reason: Optional[str] = None):
        super().__init__(reason)
        self.queue = queue

    def serialize(self) -> dict:
        return {"queue": self.queue, "enqueued_at": _iso(time.time())}


class ScheduledState(State):
    name = SCHEDULED

    def __init__(self, enqueue_at: Union[datetime, timedelta, int, float], reason: Optional[str] = None):
        super().__init__(reason)
        self.enqueue_at = to_timestamp(enqueue_at)

    def serialize(self) -> dict:
        return {"enqueue_at": _iso(self.enqueue_at), "scheduled_at": _iso(time.time())}


class ProcessingState(State):
    name = PROCESSING

    def __init__(self, server_id: str, worker_id: str, reason: Optional[str] = None):
        super().__init__(reason)
        self.server_id = server_id
        self.worker_id = worker_id

    def serialize(self) -> dict:
        return {
            "server_id": self.server_id,
            "worker_id": self.worker_id,
            "started_at": _iso(time.time()),
        }


class SucceededState(State):
    name = SUCCEEDED
    is_final = True

    def __init__(
        self,
        result=None,
        latency: Optional[float] = None,
        duration: Optional[float] = None,
        reason: Optional[str] = None,
    ):
        super().__init__(reason)
        self.result = result
        self.latency = latency
        self.duration = duration

    def serialize(self) -> dict:
        data = {"succeeded_at": _iso(time.time())}
        if self.result is not None:
            data["result"] = _serialization.to_safe_value(self.result)
        if self.latency is not None:
            data["latency_ms"] = round(self.latency * 1000)
        if self.duration is not None:
            data["duration_ms"] = round(self.duration * 1000)
        return data


class FailedState(State):
    name = FAILED

    def __init__(self, exception: BaseException, reason: Optional[str] = None):
        super().__init__(reason or "An exception occurred during performance of the job.")
        self.exception = exception
        self.exception_type = f"{type(exception).__module__}.{type(exception).__qualname__}"
        self.message = str(exception)
        self.details = "".join(
            traceback.format_exception(type(exception), exception, exception.__traceback__)
        )

    def serialize(self) -> dict:
        return {
            "failed_at": _iso(time.time()),
            "exception_type": self.exception_type,
            "exception_message": self.message,
            "exception_details": self.details,
        }


class DeletedState(State):
    name = DELETED
    is_final = True

    def serialize(self) -> dict:
        return {"deleted_at": _iso(time.time())}


class AwaitingState(State):
    """Continuation waiting for its parent job to finish."""

    name = AWAITING

    def __init__(
        self,
        parent_id: str,
        options: ContinuationOptions = ContinuationOptions.ONLY_ON_SUCCEEDED_STATE,
        reason: Optional[str] = None,
    ):
        super().__init__(reason)
        self.parent_id = str(parent_id)
        self.options = ContinuationOptions(options)

    def serialize(self) -> dict:
        return {"parent_id": self.parent_id, "options": self.options.value}

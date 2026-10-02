"""Storage abstraction.

Everything schedulefire persists goes through :class:`JobStorage`. Implement it to back
schedulefire with another database (PostgreSQL, Redis, ...); :class:`SQLiteStorage` is the
reference implementation.
"""

from __future__ import annotations

import threading
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

from ..job import Job
from ..states import State


def to_datetime(ts: Optional[float]) -> Optional[datetime]:
    return None if ts is None else datetime.fromtimestamp(ts, timezone.utc)


class DistributedLockTimeoutError(TimeoutError):
    pass


class UnsupportedDatabaseVersionError(RuntimeError):
    """The database server is older than the minimum version a storage supports."""


def version_tuple(text: str) -> tuple:
    """'8.0.36-log' -> (8, 0, 36); '10.5.27-MariaDB' -> (10, 5, 27)."""
    import re

    parts = re.findall(r"\d+", text.split("-")[0])[:3]
    return tuple(int(x) for x in parts) + (0,) * (3 - len(parts))


@dataclass
class JobRecord:
    id: str
    invocation: dict
    queue: str
    state_name: Optional[str]
    state_reason: Optional[str]
    state_data: dict
    parameters: dict
    created_at: float
    expire_at: Optional[float] = None

    @property
    def job(self) -> Job:
        return Job.from_invocation(self.invocation, queue=self.queue)


@dataclass
class StateRecord:
    name: str
    reason: Optional[str]
    data: dict
    created_at: float


@dataclass(frozen=True)
class FetchedJob:
    job_id: str
    queue: str
    #: Storage-specific handle identifying this fetch (e.g. the queue row id).
    token: Any


@dataclass
class RecurringJobRecord:
    id: str
    invocation: dict
    queue: str
    cron: str
    time_zone: str = "UTC"
    next_execution: Optional[float] = None
    last_execution: Optional[float] = None
    last_job_id: Optional[str] = None
    created_at: float = field(default_factory=time.time)
    error: Optional[str] = None

    @property
    def job(self) -> Job:
        return Job.from_invocation(self.invocation, queue=self.queue)


@dataclass
class ServerRecord:
    id: str
    name: str
    queues: List[str]
    worker_count: int
    started_at: float
    heartbeat: float
    #: A change asked for through ``request_server_change`` that the server hasn't applied yet.
    requested: Optional[Dict[str, Any]] = None


def merge_request(current: Optional[Dict[str, Any]], worker_count: Optional[int],
                  queues: Optional[Sequence[str]]) -> Dict[str, Any]:
    request = dict(current or {})
    if worker_count is not None:
        request["worker_count"] = int(worker_count)
    if queues is not None:
        request["queues"] = list(queues)
    return request


@dataclass
class QueueInfo:
    name: str
    enqueued: int
    fetched: int


class DistributedLock:
    """A held lock. Release with ``release()`` or use it as a context manager."""

    def __init__(self, storage: "JobStorage", resource: str, owner: str):
        self.storage = storage
        self.resource = resource
        self.owner = owner
        self._released = False

    def release(self) -> None:
        if not self._released:
            self._released = True
            self.storage._release_lock(self.resource, self.owner)

    def __enter__(self) -> "DistributedLock":
        return self

    def __exit__(self, *exc) -> None:
        self.release()


class JobStorage(ABC):
    def __init__(self, *, job_expiration: float = 24 * 3600):
        #: Seconds a job in a final state (Succeeded/Deleted) is kept before cleanup.
        self.job_expiration = job_expiration
        self._job_signal = threading.Condition()

    # -- in-process wake-up (cross-process consumers fall back to polling) --------

    def notify_new_jobs(self) -> None:
        with self._job_signal:
            self._job_signal.notify_all()

    def wait_for_jobs(self, timeout: float) -> None:
        with self._job_signal:
            self._job_signal.wait(timeout)

    # -- distributed locks --------------------------------------------------------

    def try_acquire_lock(self, resource: str, ttl: float = 60.0) -> Optional[DistributedLock]:
        owner = uuid.uuid4().hex
        if self._try_acquire_lock(resource, owner, ttl):
            return DistributedLock(self, resource, owner)
        return None

    def acquire_lock(self, resource: str, timeout: float, ttl: float = 60.0) -> DistributedLock:
        """Block up to ``timeout`` seconds. ``ttl`` bounds how long a crashed holder blocks others."""
        deadline = time.monotonic() + timeout
        delay = 0.05
        while True:
            lock = self.try_acquire_lock(resource, ttl)
            if lock is not None:
                return lock
            if time.monotonic() >= deadline:
                raise DistributedLockTimeoutError(
                    f"Timeout expired while acquiring lock on '{resource}'."
                )
            time.sleep(min(delay, max(0.0, deadline - time.monotonic())))
            delay = min(delay * 2, 1.0)

    @abstractmethod
    def _try_acquire_lock(self, resource: str, owner: str, ttl: float) -> bool: ...

    @abstractmethod
    def _release_lock(self, resource: str, owner: str) -> None: ...

    # -- jobs ---------------------------------------------------------------------

    @abstractmethod
    def create_job(self, invocation: dict, queue: str, parameters: Dict[str, Any], expire_in: float) -> str:
        """Insert a job with no state yet. It expires unless a state is applied."""

    @abstractmethod
    def get_job(self, job_id: str) -> Optional[JobRecord]: ...

    @abstractmethod
    def set_state(
        self,
        job_id: str,
        state: State,
        *,
        expected_states: Optional[Sequence[Optional[str]]] = None,
        expire_at: Optional[float] = None,
    ) -> bool:
        """Atomically apply ``state`` if the current state is in ``expected_states``.

        Must also: append the state to the history; remove the job from its queue
        unless the new state is Processing; add it to ``state.queue`` when the new
        state is Enqueued; record ``state.enqueue_at`` for Scheduled.
        Returns False if the job doesn't exist or the current state didn't match.
        """

    @abstractmethod
    def get_job_parameter(self, job_id: str, name: str) -> Any: ...

    @abstractmethod
    def set_job_parameter(self, job_id: str, name: str, value: Any) -> None: ...

    @abstractmethod
    def get_state_history(self, job_id: str) -> List[StateRecord]:
        """Newest first."""

    # -- queues -------------------------------------------------------------------

    @abstractmethod
    def fetch_next(self, queues: Sequence[str]) -> Optional[FetchedJob]:
        """Reserve the next job, preferring earlier queues in ``queues``.

        A reserved job becomes visible again if it isn't removed or kept alive with
        ``heartbeat_fetched`` within the invisibility timeout (crashed worker).
        """

    @abstractmethod
    def remove_from_queue(self, fetched: FetchedJob) -> None: ...

    @abstractmethod
    def heartbeat_fetched(self, tokens: Sequence[Any]) -> None: ...

    @abstractmethod
    def get_scheduled_due(self, now: float, limit: int) -> List[str]: ...

    # -- continuations ------------------------------------------------------------

    @abstractmethod
    def add_continuation(self, parent_id: str, child_id: str) -> None: ...

    @abstractmethod
    def get_continuations(self, parent_id: str) -> List[str]: ...

    # -- recurring jobs -----------------------------------------------------------

    @abstractmethod
    def upsert_recurring(self, record: RecurringJobRecord) -> None: ...

    @abstractmethod
    def get_recurring(self, recurring_id: str) -> Optional[RecurringJobRecord]: ...

    @abstractmethod
    def get_recurring_jobs(self) -> List[RecurringJobRecord]: ...

    @abstractmethod
    def get_due_recurring(self, now: float, limit: int) -> List[RecurringJobRecord]: ...

    @abstractmethod
    def update_recurring(self, recurring_id: str, **fields: Any) -> None: ...

    @abstractmethod
    def remove_recurring(self, recurring_id: str) -> bool: ...

    # -- servers ------------------------------------------------------------------

    @abstractmethod
    def announce_server(self, server_id: str, name: str, queues: Sequence[str], worker_count: int) -> None: ...

    @abstractmethod
    def server_heartbeat(self, server_id: str) -> None: ...

    @abstractmethod
    def remove_server(self, server_id: str) -> None: ...

    @abstractmethod
    def remove_timed_out_servers(self, timeout: float) -> int: ...

    @abstractmethod
    def get_servers(self) -> List[ServerRecord]: ...

    @abstractmethod
    def get_server(self, server_id: str) -> Optional[ServerRecord]: ...

    @abstractmethod
    def request_server_change(self, server_id: str, worker_count: Optional[int] = None,
                              queues: Optional[Sequence[str]] = None) -> bool:
        """Ask a running server (possibly in another process) to change its workers/queues.

        Merges into any pending request. Returns False if the server isn't registered.
        """

    @abstractmethod
    def update_server_state(self, server_id: str, worker_count: int, queues: Sequence[str],
                            applied_request: Optional[Dict[str, Any]] = None) -> None:
        """Record a server's actual settings. Clears the pending request if it equals ``applied_request``."""

    # -- maintenance --------------------------------------------------------------

    @abstractmethod
    def delete_expired_jobs(self, now: float, batch_size: int = 1000) -> int: ...

    # -- monitoring (dashboard) ---------------------------------------------------

    @abstractmethod
    def get_statistics(self) -> Dict[str, int]: ...

    @abstractmethod
    def get_jobs(self, state: str, offset: int = 0, limit: int = 20) -> List[JobRecord]: ...

    @abstractmethod
    def count_jobs(self, state: str) -> int: ...

    @abstractmethod
    def get_queues(self) -> List[QueueInfo]: ...

    @abstractmethod
    def get_daily_history(self, days: int = 7) -> List[Dict[str, Any]]:
        """``[{"date": "YYYY-MM-DD", "succeeded": n, "failed": n}, ...]`` oldest first."""

    def get_hourly_history(self, hours: int = 24) -> List[Dict[str, Any]]:
        """``[{"hour": "YYYY-MM-DDTHH", "succeeded": n, "failed": n}, ...]`` oldest first (UTC).

        Optional: storages that don't track hourly counts report zeros.
        """
        now = datetime.now(timezone.utc)
        return [
            {"hour": (now - timedelta(hours=i)).strftime("%Y-%m-%dT%H"), "succeeded": 0, "failed": 0}
            for i in range(hours - 1, -1, -1)
        ]

    def close(self) -> None:
        """Release resources held for the *calling thread* (e.g. its DB connection).

        Server threads call this when they exit. Safe to call at any time.
        """

    def dispose(self) -> None:
        """Release everything (listener threads, connection pools). The storage is unusable after."""
        self.close()

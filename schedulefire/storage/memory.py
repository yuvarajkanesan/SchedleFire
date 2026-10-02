"""In-memory job storage - no database required.

Everything lives in this process's memory, so it is only for:

* tests and local experiments;
* single-process apps where the web app and the ``BackgroundJobServer`` run in the
  same Python process, and losing pending jobs on restart is acceptable.

Jobs enqueued in one process are invisible to any other process, and everything is
lost when the process exits. Use SQLite, PostgreSQL or Redis for anything else.
"""

from __future__ import annotations

import copy
import itertools
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

from .. import _serialization
from ..states import ALL_STATES, DELETED, ENQUEUED, FAILED, PROCESSING, SCHEDULED, SUCCEEDED, State
from .base import (
    FetchedJob,
    JobRecord,
    JobStorage,
    QueueInfo,
    RecurringJobRecord,
    ServerRecord,
    StateRecord,
    merge_request,
)


def _day(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")


def _hour(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H")


class MemoryStorage(JobStorage):
    def __init__(self, *, invisibility_timeout: float = 30 * 60, job_expiration: float = 24 * 3600):
        super().__init__(job_expiration=job_expiration)
        self.invisibility_timeout = invisibility_timeout
        self._lock = threading.RLock()
        self._ids = itertools.count(1)
        self._queue_ids = itertools.count(1)
        self._jobs: Dict[int, dict] = {}
        self._history: Dict[int, List[StateRecord]] = {}
        self._queue: Dict[int, dict] = {}  # queue entry id -> {job_id, queue, fetched_at}
        self._continuations: Dict[int, List[int]] = {}
        self._recurring: Dict[str, RecurringJobRecord] = {}
        self._servers: Dict[str, ServerRecord] = {}
        self._locks: Dict[str, tuple] = {}
        self._counters: Dict[str, int] = {}

    @staticmethod
    def _int_id(job_id: Any) -> Optional[int]:
        try:
            return int(job_id)
        except (TypeError, ValueError):
            return None

    # -- locks --------------------------------------------------------------------

    def _try_acquire_lock(self, resource: str, owner: str, ttl: float) -> bool:
        now = time.time()
        with self._lock:
            held = self._locks.get(resource)
            if held is not None and held[1] >= now:
                return False
            self._locks[resource] = (owner, now + ttl)
            return True

    def _release_lock(self, resource: str, owner: str) -> None:
        with self._lock:
            if self._locks.get(resource, (None,))[0] == owner:
                del self._locks[resource]

    # -- jobs ---------------------------------------------------------------------

    def _record(self, jid: int, row: dict) -> JobRecord:
        return JobRecord(
            id=str(jid),
            invocation=_serialization.loads(row["invocation"]),
            queue=row["queue"],
            state_name=row["state"],
            state_reason=row["state_reason"],
            state_data=_serialization.loads(row["state_data"]) if row["state_data"] else {},
            parameters=_serialization.loads(row["params"]),
            created_at=row["created_at"],
            expire_at=row["expire_at"],
        )

    def create_job(self, invocation: dict, queue: str, parameters: Dict[str, Any], expire_in: float) -> str:
        now = time.time()
        with self._lock:
            jid = next(self._ids)
            self._jobs[jid] = {
                "invocation": _serialization.dumps(invocation), "queue": queue, "state": None,
                "state_reason": None, "state_data": None, "params": _serialization.dumps(parameters),
                "created_at": now, "enqueue_at": None, "expire_at": now + expire_in,
            }
            self._history[jid] = []
            return str(jid)

    def get_job(self, job_id: str) -> Optional[JobRecord]:
        jid = self._int_id(job_id)
        with self._lock:
            row = self._jobs.get(jid)
            return None if row is None else self._record(jid, row)

    def set_state(self, job_id, state: State, *, expected_states=None, expire_at=None) -> bool:
        jid = self._int_id(job_id)
        now = time.time()
        with self._lock:
            row = self._jobs.get(jid)
            if row is None:
                return False
            if expected_states is not None and row["state"] not in expected_states:
                return False
            data = _serialization.dumps(state.serialize())
            row.update(state=state.name, state_reason=state.reason, state_data=data, expire_at=expire_at,
                       enqueue_at=getattr(state, "enqueue_at", None) if state.name == SCHEDULED else None)
            self._history[jid].insert(0, StateRecord(state.name, state.reason, _serialization.loads(data), now))
            if state.name != PROCESSING:
                for qid in [q for q, e in self._queue.items() if e["job_id"] == jid]:
                    del self._queue[qid]
            if state.name == ENQUEUED:
                self._queue[next(self._queue_ids)] = {"job_id": jid, "queue": state.queue, "fetched_at": None}
            if state.name in (SUCCEEDED, FAILED, DELETED):
                name = state.name.lower()
                for key in (f"stats:{name}", f"stats:{name}:{_day(now)}", f"stats:{name}:h:{_hour(now)}"):
                    self._counters[key] = self._counters.get(key, 0) + 1
        if state.name == ENQUEUED:
            self.notify_new_jobs()
        return True

    def get_job_parameter(self, job_id: str, name: str) -> Any:
        record = self.get_job(job_id)
        return None if record is None else record.parameters.get(name)

    def set_job_parameter(self, job_id: str, name: str, value: Any) -> None:
        jid = self._int_id(job_id)
        with self._lock:
            row = self._jobs.get(jid)
            if row is not None:
                params = _serialization.loads(row["params"])
                params[name] = value
                row["params"] = _serialization.dumps(params)

    def get_state_history(self, job_id: str) -> List[StateRecord]:
        with self._lock:
            return copy.deepcopy(self._history.get(self._int_id(job_id), []))

    # -- queues -------------------------------------------------------------------

    def fetch_next(self, queues: Sequence[str]) -> Optional[FetchedJob]:
        now = time.time()
        cutoff = now - self.invisibility_timeout
        priority = {q: i for i, q in enumerate(queues)}
        with self._lock:
            candidates = [
                (priority[e["queue"]], qid) for qid, e in self._queue.items()
                if e["queue"] in priority and (e["fetched_at"] is None or e["fetched_at"] < cutoff)
            ]
            if not candidates:
                return None
            _, qid = min(candidates)
            entry = self._queue[qid]
            entry["fetched_at"] = now
            return FetchedJob(job_id=str(entry["job_id"]), queue=entry["queue"], token=qid)

    def remove_from_queue(self, fetched: FetchedJob) -> None:
        with self._lock:
            self._queue.pop(fetched.token, None)

    def heartbeat_fetched(self, tokens: Sequence[Any]) -> None:
        now = time.time()
        with self._lock:
            for token in tokens:
                if token in self._queue:
                    self._queue[token]["fetched_at"] = now

    def get_scheduled_due(self, now: float, limit: int) -> List[str]:
        with self._lock:
            due = sorted((row["enqueue_at"], jid) for jid, row in self._jobs.items()
                         if row["state"] == SCHEDULED and row["enqueue_at"] is not None and row["enqueue_at"] <= now)
            return [str(jid) for _, jid in due[:limit]]

    # -- continuations ------------------------------------------------------------

    def add_continuation(self, parent_id: str, child_id: str) -> None:
        with self._lock:
            children = self._continuations.setdefault(self._int_id(parent_id), [])
            child = self._int_id(child_id)
            if child not in children:
                children.append(child)

    def get_continuations(self, parent_id: str) -> List[str]:
        with self._lock:
            return [str(c) for c in sorted(self._continuations.get(self._int_id(parent_id), []))]

    # -- recurring jobs -----------------------------------------------------------

    def upsert_recurring(self, record: RecurringJobRecord) -> None:
        with self._lock:
            existing = self._recurring.get(record.id)
            record = copy.deepcopy(record)
            if existing is not None:
                record.created_at = existing.created_at
            self._recurring[record.id] = record

    def get_recurring(self, recurring_id: str) -> Optional[RecurringJobRecord]:
        with self._lock:
            return copy.deepcopy(self._recurring.get(recurring_id))

    def get_recurring_jobs(self) -> List[RecurringJobRecord]:
        with self._lock:
            return [copy.deepcopy(r) for _, r in sorted(self._recurring.items())]

    def get_due_recurring(self, now: float, limit: int) -> List[RecurringJobRecord]:
        with self._lock:
            due = sorted((r for r in self._recurring.values()
                          if r.next_execution is not None and r.next_execution <= now),
                         key=lambda r: r.next_execution)
            return [copy.deepcopy(r) for r in due[:limit]]

    def update_recurring(self, recurring_id: str, **fields: Any) -> None:
        allowed = {"invocation", "queue", "cron", "time_zone", "next_execution", "last_execution",
                   "last_job_id", "error"}
        unknown = set(fields) - allowed
        if unknown:
            raise ValueError(f"Unknown recurring job fields: {sorted(unknown)}")
        with self._lock:
            record = self._recurring.get(recurring_id)
            if record is not None:
                for name, value in fields.items():
                    setattr(record, name, copy.deepcopy(value))

    def remove_recurring(self, recurring_id: str) -> bool:
        with self._lock:
            return self._recurring.pop(recurring_id, None) is not None

    # -- servers ------------------------------------------------------------------

    def announce_server(self, server_id: str, name: str, queues: Sequence[str], worker_count: int) -> None:
        now = time.time()
        with self._lock:
            self._servers[server_id] = ServerRecord(server_id, name, list(queues), worker_count, now, now)

    def server_heartbeat(self, server_id: str) -> None:
        with self._lock:
            if server_id in self._servers:
                self._servers[server_id].heartbeat = time.time()

    def remove_server(self, server_id: str) -> None:
        with self._lock:
            self._servers.pop(server_id, None)

    def remove_timed_out_servers(self, timeout: float) -> int:
        cutoff = time.time() - timeout
        with self._lock:
            dead = [sid for sid, s in self._servers.items() if s.heartbeat < cutoff]
            for sid in dead:
                del self._servers[sid]
            return len(dead)

    def get_servers(self) -> List[ServerRecord]:
        with self._lock:
            return sorted((copy.deepcopy(s) for s in self._servers.values()), key=lambda s: s.started_at)

    def get_server(self, server_id: str) -> Optional[ServerRecord]:
        with self._lock:
            return copy.deepcopy(self._servers.get(server_id))

    def request_server_change(self, server_id, worker_count=None, queues=None) -> bool:
        with self._lock:
            server = self._servers.get(server_id)
            if server is None:
                return False
            server.requested = merge_request(server.requested, worker_count, queues)
            return True

    def update_server_state(self, server_id, worker_count, queues, applied_request=None) -> None:
        with self._lock:
            server = self._servers.get(server_id)
            if server is not None:
                server.worker_count, server.queues = worker_count, list(queues)
                if applied_request is not None and server.requested == applied_request:
                    server.requested = None

    # -- maintenance --------------------------------------------------------------

    def delete_expired_jobs(self, now: float, batch_size: int = 1000) -> int:
        with self._lock:
            self._locks = {k: v for k, v in self._locks.items() if v[1] >= now}
            cutoff = _hour(now - 7 * 86400)
            self._counters = {k: v for k, v in self._counters.items() if ":h:" not in k or k[-13:] >= cutoff}
            expired = [jid for jid, row in self._jobs.items()
                       if row["expire_at"] is not None and row["expire_at"] < now][:batch_size]
            for jid in expired:
                del self._jobs[jid]
                self._history.pop(jid, None)
                self._continuations.pop(jid, None)
                for qid in [q for q, e in self._queue.items() if e["job_id"] == jid]:
                    del self._queue[qid]
            for children in self._continuations.values():
                children[:] = [c for c in children if c not in expired]
            return len(expired)

    # -- monitoring ---------------------------------------------------------------

    def get_statistics(self) -> Dict[str, int]:
        with self._lock:
            stats = {name.lower(): 0 for name in ALL_STATES}
            for row in self._jobs.values():
                if row["state"]:
                    stats[row["state"].lower()] += 1
            stats["succeeded"] = self._counters.get("stats:succeeded", 0)
            stats["deleted"] = self._counters.get("stats:deleted", 0)
            stats["servers"] = len(self._servers)
            stats["recurring"] = len(self._recurring)
            stats["queues"] = len({e["queue"] for e in self._queue.values()})
            return stats

    def get_jobs(self, state: str, offset: int = 0, limit: int = 20) -> List[JobRecord]:
        with self._lock:
            rows = [(jid, row) for jid, row in self._jobs.items() if row["state"] == state]
            if state == SCHEDULED:
                rows.sort(key=lambda r: (r[1]["enqueue_at"] or 0, r[0]))
            elif state == ENQUEUED:
                rows.sort(key=lambda r: r[0])
            else:
                rows.sort(key=lambda r: -r[0])
            return [self._record(jid, row) for jid, row in rows[offset:offset + limit]]

    def count_jobs(self, state: str) -> int:
        with self._lock:
            return sum(1 for row in self._jobs.values() if row["state"] == state)

    def get_queues(self) -> List[QueueInfo]:
        with self._lock:
            info: Dict[str, List[int]] = {}
            for e in self._queue.values():
                counts = info.setdefault(e["queue"], [0, 0])
                counts[0 if e["fetched_at"] is None else 1] += 1
            return [QueueInfo(q, c[0], c[1]) for q, c in sorted(info.items())]

    def get_daily_history(self, days: int = 7) -> List[Dict[str, Any]]:
        today = datetime.now(timezone.utc).date()
        with self._lock:
            return [
                {"date": d, "succeeded": self._counters.get(f"stats:succeeded:{d}", 0),
                 "failed": self._counters.get(f"stats:failed:{d}", 0)}
                for d in ((today - timedelta(days=i)).isoformat() for i in range(days - 1, -1, -1))
            ]

    def get_hourly_history(self, hours: int = 24) -> List[Dict[str, Any]]:
        now = time.time()
        with self._lock:
            return [
                {"hour": hr, "succeeded": self._counters.get(f"stats:succeeded:h:{hr}", 0),
                 "failed": self._counters.get(f"stats:failed:h:{hr}", 0)}
                for hr in (_hour(now - i * 3600) for i in range(hours - 1, -1, -1))
            ]

"""SQLite job storage.

Safe for many worker threads and several processes on the same machine sharing one
database file (WAL mode + ``BEGIN IMMEDIATE`` for writes). Don't put the file on a
network share.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional, Sequence

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
    UnsupportedDatabaseVersionError,
    merge_request,
)

#: WAL mode and partial indexes need SQLite 3.8.0 (2013).
MIN_SQLITE_VERSION = (3, 8, 0)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS job (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    invocation   TEXT NOT NULL,
    queue        TEXT NOT NULL,
    state        TEXT,
    state_reason TEXT,
    state_data   TEXT,
    params       TEXT NOT NULL DEFAULT '{}',
    created_at   REAL NOT NULL,
    enqueue_at   REAL,
    expire_at    REAL
);
CREATE INDEX IF NOT EXISTS ix_job_state ON job(state, id);
CREATE INDEX IF NOT EXISTS ix_job_scheduled ON job(state, enqueue_at);
CREATE INDEX IF NOT EXISTS ix_job_expire ON job(expire_at) WHERE expire_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS state_history (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id     INTEGER NOT NULL,
    name       TEXT NOT NULL,
    reason     TEXT,
    data       TEXT,
    created_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_state_history_job ON state_history(job_id);

CREATE TABLE IF NOT EXISTS job_queue (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id     INTEGER NOT NULL,
    queue      TEXT NOT NULL,
    fetched_at REAL
);
CREATE INDEX IF NOT EXISTS ix_job_queue_fetch ON job_queue(queue, fetched_at, id);
CREATE INDEX IF NOT EXISTS ix_job_queue_job ON job_queue(job_id);

CREATE TABLE IF NOT EXISTS continuation (
    parent_id INTEGER NOT NULL,
    child_id  INTEGER NOT NULL,
    PRIMARY KEY (parent_id, child_id)
);
CREATE INDEX IF NOT EXISTS ix_continuation_child ON continuation(child_id);

CREATE TABLE IF NOT EXISTS recurring_job (
    id             TEXT PRIMARY KEY,
    invocation     TEXT NOT NULL,
    queue          TEXT NOT NULL,
    cron           TEXT NOT NULL,
    time_zone      TEXT NOT NULL,
    next_execution REAL,
    last_execution REAL,
    last_job_id    TEXT,
    created_at     REAL NOT NULL,
    error          TEXT
);
CREATE INDEX IF NOT EXISTS ix_recurring_next ON recurring_job(next_execution);

CREATE TABLE IF NOT EXISTS server (
    id         TEXT PRIMARY KEY,
    data       TEXT NOT NULL,
    started_at REAL NOT NULL,
    heartbeat  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS lock (
    resource  TEXT PRIMARY KEY,
    owner     TEXT NOT NULL,
    expire_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS counter (
    key   TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
"""

_RECURRING_FIELDS = {
    "invocation", "queue", "cron", "time_zone", "next_execution",
    "last_execution", "last_job_id", "error",
}


def _int_id(job_id: Any) -> Optional[int]:
    try:
        return int(job_id)
    except (TypeError, ValueError):
        return None


def _day(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")


def _hour(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H")


class SQLiteStorage(JobStorage):
    def __init__(
        self,
        path: "str | os.PathLike[str]" = "schedulefire.db",
        *,
        invisibility_timeout: float = 30 * 60,
        job_expiration: float = 24 * 3600,
        busy_timeout: float = 30.0,
    ):
        super().__init__(job_expiration=job_expiration)
        path = os.fspath(path)
        if path in ("", ":memory:") or path.startswith("file::memory:"):
            raise ValueError(
                "SQLiteStorage needs a file path: each worker thread opens its own "
                "connection, so an in-memory database would not be shared."
            )
        if sqlite3.sqlite_version_info < MIN_SQLITE_VERSION:
            raise UnsupportedDatabaseVersionError(
                f"SQLiteStorage needs SQLite {'.'.join(map(str, MIN_SQLITE_VERSION))} or newer; this Python "
                f"is linked against SQLite {sqlite3.sqlite_version}."
            )
        # Create missing folders so a fresh path just works.
        folder = os.path.dirname(os.path.abspath(path))
        os.makedirs(folder, exist_ok=True)
        self.path = path
        #: Seconds before a job reserved by a worker that stopped heart-beating is re-fetched.
        self.invisibility_timeout = invisibility_timeout
        self._busy_timeout = busy_timeout
        self._local = threading.local()  # one connection per thread
        is_new = not os.path.exists(path)
        conn = self._conn()
        if is_new and os.name == "posix":
            # Job arguments may hold personal data: owner-only access. SQLite gives the
            # -wal/-shm files the same permissions as the database file.
            os.chmod(path, 0o600)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(_SCHEMA)

    # -- connections --------------------------------------------------------------

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is not None and self._local.pid != os.getpid():
            # Inherited through fork() (gunicorn/uWSGI --preload): never reuse it in the child.
            conn = None
        if conn is None:
            conn = sqlite3.connect(
                self.path,
                timeout=self._busy_timeout,
                isolation_level=None,  # we manage transactions explicitly
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
            self._local.pid = os.getpid()
        return conn

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")

    def close(self) -> None:
        """Close this thread's connection. Other threads' connections close when those threads exit."""
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            self._local.conn = None
            conn.close()

    # -- locks --------------------------------------------------------------------

    def _try_acquire_lock(self, resource: str, owner: str, ttl: float) -> bool:
        now = time.time()
        with self._write() as c:
            c.execute("DELETE FROM lock WHERE resource = ? AND expire_at < ?", (resource, now))
            cur = c.execute(
                "INSERT OR IGNORE INTO lock (resource, owner, expire_at) VALUES (?, ?, ?)",
                (resource, owner, now + ttl),
            )
            return cur.rowcount == 1

    def _release_lock(self, resource: str, owner: str) -> None:
        with self._write() as c:
            c.execute("DELETE FROM lock WHERE resource = ? AND owner = ?", (resource, owner))

    # -- jobs ---------------------------------------------------------------------

    @staticmethod
    def _job_record(row: sqlite3.Row) -> JobRecord:
        return JobRecord(
            id=str(row["id"]),
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
        with self._write() as c:
            cur = c.execute(
                "INSERT INTO job (invocation, queue, params, created_at, expire_at) VALUES (?, ?, ?, ?, ?)",
                (_serialization.dumps(invocation), queue, _serialization.dumps(parameters), now, now + expire_in),
            )
            return str(cur.lastrowid)

    def get_job(self, job_id: str) -> Optional[JobRecord]:
        jid = _int_id(job_id)
        if jid is None:
            return None
        row = self._conn().execute("SELECT * FROM job WHERE id = ?", (jid,)).fetchone()
        return self._job_record(row) if row else None

    def set_state(self, job_id, state: State, *, expected_states=None, expire_at=None) -> bool:
        jid = _int_id(job_id)
        if jid is None:
            return False
        now = time.time()
        with self._write() as c:
            row = c.execute("SELECT state FROM job WHERE id = ?", (jid,)).fetchone()
            if row is None:
                return False
            if expected_states is not None and row["state"] not in expected_states:
                return False
            data = _serialization.dumps(state.serialize())
            enqueue_at = getattr(state, "enqueue_at", None) if state.name == SCHEDULED else None
            c.execute(
                "UPDATE job SET state = ?, state_reason = ?, state_data = ?, enqueue_at = ?, expire_at = ? WHERE id = ?",
                (state.name, state.reason, data, enqueue_at, expire_at, jid),
            )
            c.execute(
                "INSERT INTO state_history (job_id, name, reason, data, created_at) VALUES (?, ?, ?, ?, ?)",
                (jid, state.name, state.reason, data, now),
            )
            if state.name != PROCESSING:
                c.execute("DELETE FROM job_queue WHERE job_id = ?", (jid,))
            if state.name == ENQUEUED:
                c.execute("INSERT INTO job_queue (job_id, queue) VALUES (?, ?)", (jid, state.queue))
            if state.name in (SUCCEEDED, FAILED, DELETED):
                name = state.name.lower()
                for key in (f"stats:{name}", f"stats:{name}:{_day(now)}", f"stats:{name}:h:{_hour(now)}"):
                    # Two statements instead of an upsert: works on SQLite older than 3.24.
                    c.execute("INSERT OR IGNORE INTO counter (key, value) VALUES (?, 0)", (key,))
                    c.execute("UPDATE counter SET value = value + 1 WHERE key = ?", (key,))
        if state.name == ENQUEUED:
            self.notify_new_jobs()
        return True

    def get_job_parameter(self, job_id: str, name: str) -> Any:
        record = self.get_job(job_id)
        return None if record is None else record.parameters.get(name)

    def set_job_parameter(self, job_id: str, name: str, value: Any) -> None:
        jid = _int_id(job_id)
        if jid is None:
            return
        with self._write() as c:
            row = c.execute("SELECT params FROM job WHERE id = ?", (jid,)).fetchone()
            if row is None:
                return
            params = _serialization.loads(row["params"])
            params[name] = value
            c.execute("UPDATE job SET params = ? WHERE id = ?", (_serialization.dumps(params), jid))

    def get_state_history(self, job_id: str) -> List[StateRecord]:
        jid = _int_id(job_id)
        if jid is None:
            return []
        rows = self._conn().execute(
            "SELECT name, reason, data, created_at FROM state_history WHERE job_id = ? ORDER BY id DESC",
            (jid,),
        ).fetchall()
        return [
            StateRecord(r["name"], r["reason"], _serialization.loads(r["data"]) if r["data"] else {}, r["created_at"])
            for r in rows
        ]

    # -- queues -------------------------------------------------------------------

    def fetch_next(self, queues: Sequence[str]) -> Optional[FetchedJob]:
        if not queues:
            return None
        placeholders = ",".join("?" * len(queues))
        priority = "CASE queue " + " ".join(f"WHEN ? THEN {i}" for i in range(len(queues))) + " END"
        sql = (
            f"SELECT id, job_id, queue FROM job_queue "
            f"WHERE queue IN ({placeholders}) AND (fetched_at IS NULL OR fetched_at < ?) "
            f"ORDER BY {priority}, id LIMIT 1"
        )
        now = time.time()
        params = [*queues, now - self.invisibility_timeout, *queues]
        # Cheap read first so idle workers don't take the write lock on every poll.
        if self._conn().execute(sql, params).fetchone() is None:
            return None
        with self._write() as c:
            row = c.execute(sql, params).fetchone()
            if row is None:
                return None
            c.execute("UPDATE job_queue SET fetched_at = ? WHERE id = ?", (now, row["id"]))
        return FetchedJob(job_id=str(row["job_id"]), queue=row["queue"], token=row["id"])

    def remove_from_queue(self, fetched: FetchedJob) -> None:
        with self._write() as c:
            c.execute("DELETE FROM job_queue WHERE id = ?", (fetched.token,))

    def heartbeat_fetched(self, tokens: Sequence[Any]) -> None:
        tokens = list(tokens)
        if not tokens:
            return
        with self._write() as c:
            c.execute(
                f"UPDATE job_queue SET fetched_at = ? WHERE id IN ({','.join('?' * len(tokens))})",
                (time.time(), *tokens),
            )

    def get_scheduled_due(self, now: float, limit: int) -> List[str]:
        rows = self._conn().execute(
            "SELECT id FROM job WHERE state = ? AND enqueue_at <= ? ORDER BY enqueue_at LIMIT ?",
            (SCHEDULED, now, limit),
        ).fetchall()
        return [str(r["id"]) for r in rows]

    # -- continuations ------------------------------------------------------------

    def add_continuation(self, parent_id: str, child_id: str) -> None:
        with self._write() as c:
            c.execute(
                "INSERT OR IGNORE INTO continuation (parent_id, child_id) VALUES (?, ?)",
                (_int_id(parent_id), _int_id(child_id)),
            )

    def get_continuations(self, parent_id: str) -> List[str]:
        rows = self._conn().execute(
            "SELECT child_id FROM continuation WHERE parent_id = ? ORDER BY child_id", (_int_id(parent_id),)
        ).fetchall()
        return [str(r["child_id"]) for r in rows]

    # -- recurring jobs -----------------------------------------------------------

    @staticmethod
    def _recurring_record(row: sqlite3.Row) -> RecurringJobRecord:
        return RecurringJobRecord(
            id=row["id"],
            invocation=_serialization.loads(row["invocation"]),
            queue=row["queue"],
            cron=row["cron"],
            time_zone=row["time_zone"],
            next_execution=row["next_execution"],
            last_execution=row["last_execution"],
            last_job_id=row["last_job_id"],
            created_at=row["created_at"],
            error=row["error"],
        )

    def upsert_recurring(self, record: RecurringJobRecord) -> None:
        values = (
            _serialization.dumps(record.invocation), record.queue, record.cron, record.time_zone,
            record.next_execution, record.last_execution, record.last_job_id, record.error,
        )
        with self._write() as c:
            c.execute("INSERT OR IGNORE INTO recurring_job (id, invocation, queue, cron, time_zone, created_at) "
                      "VALUES (?, ?, ?, ?, ?, ?)",
                      (record.id, values[0], record.queue, record.cron, record.time_zone, record.created_at))
            c.execute("UPDATE recurring_job SET invocation = ?, queue = ?, cron = ?, time_zone = ?, "
                      "next_execution = ?, last_execution = ?, last_job_id = ?, error = ? WHERE id = ?",
                      (*values, record.id))

    def get_recurring(self, recurring_id: str) -> Optional[RecurringJobRecord]:
        row = self._conn().execute("SELECT * FROM recurring_job WHERE id = ?", (recurring_id,)).fetchone()
        return self._recurring_record(row) if row else None

    def get_recurring_jobs(self) -> List[RecurringJobRecord]:
        rows = self._conn().execute("SELECT * FROM recurring_job ORDER BY id").fetchall()
        return [self._recurring_record(r) for r in rows]

    def get_due_recurring(self, now: float, limit: int) -> List[RecurringJobRecord]:
        rows = self._conn().execute(
            "SELECT * FROM recurring_job WHERE next_execution IS NOT NULL AND next_execution <= ? "
            "ORDER BY next_execution LIMIT ?",
            (now, limit),
        ).fetchall()
        return [self._recurring_record(r) for r in rows]

    def update_recurring(self, recurring_id: str, **fields: Any) -> None:
        unknown = set(fields) - _RECURRING_FIELDS
        if unknown:
            raise ValueError(f"Unknown recurring job fields: {sorted(unknown)}")
        if not fields:
            return
        if "invocation" in fields:
            fields["invocation"] = _serialization.dumps(fields["invocation"])
        assignments = ", ".join(f"{name} = ?" for name in fields)
        with self._write() as c:
            c.execute(f"UPDATE recurring_job SET {assignments} WHERE id = ?", (*fields.values(), recurring_id))

    def remove_recurring(self, recurring_id: str) -> bool:
        with self._write() as c:
            return c.execute("DELETE FROM recurring_job WHERE id = ?", (recurring_id,)).rowcount > 0

    # -- servers ------------------------------------------------------------------

    def announce_server(self, server_id: str, name: str, queues: Sequence[str], worker_count: int) -> None:
        now = time.time()
        data = json.dumps({"name": name, "queues": list(queues), "worker_count": worker_count})
        with self._write() as c:
            c.execute(
                "INSERT OR REPLACE INTO server (id, data, started_at, heartbeat) VALUES (?, ?, ?, ?)",
                (server_id, data, now, now),
            )

    def server_heartbeat(self, server_id: str) -> None:
        with self._write() as c:
            c.execute("UPDATE server SET heartbeat = ? WHERE id = ?", (time.time(), server_id))

    def remove_server(self, server_id: str) -> None:
        with self._write() as c:
            c.execute("DELETE FROM server WHERE id = ?", (server_id,))

    def remove_timed_out_servers(self, timeout: float) -> int:
        with self._write() as c:
            return c.execute("DELETE FROM server WHERE heartbeat < ?", (time.time() - timeout,)).rowcount

    def _server_record(self, r) -> ServerRecord:
        data = json.loads(r["data"])
        return ServerRecord(
            id=r["id"], name=data["name"], queues=data["queues"], worker_count=data["worker_count"],
            started_at=r["started_at"], heartbeat=r["heartbeat"], requested=data.get("requested"),
        )

    def get_servers(self) -> List[ServerRecord]:
        rows = self._conn().execute("SELECT * FROM server ORDER BY started_at").fetchall()
        return [self._server_record(r) for r in rows]

    def get_server(self, server_id: str) -> Optional[ServerRecord]:
        row = self._conn().execute("SELECT * FROM server WHERE id = ?", (server_id,)).fetchone()
        return self._server_record(row) if row else None

    def request_server_change(self, server_id, worker_count=None, queues=None) -> bool:
        with self._write() as c:
            row = c.execute("SELECT data FROM server WHERE id = ?", (server_id,)).fetchone()
            if row is None:
                return False
            data = json.loads(row["data"])
            data["requested"] = merge_request(data.get("requested"), worker_count, queues)
            c.execute("UPDATE server SET data = ? WHERE id = ?", (json.dumps(data), server_id))
            return True

    def update_server_state(self, server_id, worker_count, queues, applied_request=None) -> None:
        with self._write() as c:
            row = c.execute("SELECT data FROM server WHERE id = ?", (server_id,)).fetchone()
            if row is None:
                return
            data = json.loads(row["data"])
            data.update(worker_count=worker_count, queues=list(queues))
            if applied_request is not None and data.get("requested") == applied_request:
                data.pop("requested", None)
            c.execute("UPDATE server SET data = ? WHERE id = ?", (json.dumps(data), server_id))

    # -- maintenance --------------------------------------------------------------

    def delete_expired_jobs(self, now: float, batch_size: int = 1000) -> int:
        with self._write() as c:
            c.execute("DELETE FROM lock WHERE expire_at < ?", (now,))
            # Hourly counters only feed the 24h chart; keep a week of them.
            c.execute("DELETE FROM counter WHERE key LIKE 'stats:%:h:%' AND substr(key, -13) < ?",
                      (_hour(now - 7 * 86400),))
            ids = [r["id"] for r in c.execute(
                "SELECT id FROM job WHERE expire_at IS NOT NULL AND expire_at < ? LIMIT ?", (now, batch_size)
            ).fetchall()]
            if not ids:
                return 0
            marks = ",".join("?" * len(ids))
            c.execute(f"DELETE FROM state_history WHERE job_id IN ({marks})", ids)
            c.execute(f"DELETE FROM job_queue WHERE job_id IN ({marks})", ids)
            c.execute(f"DELETE FROM continuation WHERE parent_id IN ({marks}) OR child_id IN ({marks})", ids + ids)
            c.execute(f"DELETE FROM job WHERE id IN ({marks})", ids)
            return len(ids)

    # -- monitoring ---------------------------------------------------------------

    def _counter(self, key: str) -> int:
        row = self._conn().execute("SELECT value FROM counter WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else 0

    def get_statistics(self) -> Dict[str, int]:
        c = self._conn()
        counts = {r["state"]: r["n"] for r in c.execute(
            "SELECT state, COUNT(*) AS n FROM job WHERE state IS NOT NULL GROUP BY state"
        )}
        stats = {name.lower(): counts.get(name, 0) for name in ALL_STATES}
        # Succeeded/Deleted jobs expire, so show all-time totals for them like Hangfire does.
        stats["succeeded"] = self._counter("stats:succeeded")
        stats["deleted"] = self._counter("stats:deleted")
        stats["servers"] = c.execute("SELECT COUNT(*) FROM server").fetchone()[0]
        stats["recurring"] = c.execute("SELECT COUNT(*) FROM recurring_job").fetchone()[0]
        stats["queues"] = c.execute("SELECT COUNT(DISTINCT queue) FROM job_queue").fetchone()[0]
        return stats

    def get_jobs(self, state: str, offset: int = 0, limit: int = 20) -> List[JobRecord]:
        order = {
            SCHEDULED: "enqueue_at ASC, id ASC",
            ENQUEUED: "id ASC",
        }.get(state, "id DESC")
        rows = self._conn().execute(
            f"SELECT * FROM job WHERE state = ? ORDER BY {order} LIMIT ? OFFSET ?", (state, limit, offset)
        ).fetchall()
        return [self._job_record(r) for r in rows]

    def count_jobs(self, state: str) -> int:
        return self._conn().execute("SELECT COUNT(*) FROM job WHERE state = ?", (state,)).fetchone()[0]

    def get_queues(self) -> List[QueueInfo]:
        rows = self._conn().execute(
            "SELECT queue, SUM(fetched_at IS NULL) AS enqueued, SUM(fetched_at IS NOT NULL) AS fetched "
            "FROM job_queue GROUP BY queue ORDER BY queue"
        ).fetchall()
        return [QueueInfo(r["queue"], r["enqueued"], r["fetched"]) for r in rows]

    def get_hourly_history(self, hours: int = 24) -> List[Dict[str, Any]]:
        now = time.time()
        result = []
        for i in range(hours - 1, -1, -1):
            hour = _hour(now - i * 3600)
            result.append({
                "hour": hour,
                "succeeded": self._counter(f"stats:succeeded:h:{hour}"),
                "failed": self._counter(f"stats:failed:h:{hour}"),
            })
        return result

    def get_daily_history(self, days: int = 7) -> List[Dict[str, Any]]:
        today = datetime.now(timezone.utc).date()
        result = []
        for i in range(days - 1, -1, -1):
            day = (today - timedelta(days=i)).isoformat()
            result.append({
                "date": day,
                "succeeded": self._counter(f"stats:succeeded:{day}"),
                "failed": self._counter(f"stats:failed:{day}"),
            })
        return result

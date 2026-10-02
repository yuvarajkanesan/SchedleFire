"""Shared implementation for client/server SQL databases reached through DB-API drivers
(MySQL, SQL Server). Subclasses supply the dialect: connecting, quoting, paging, upserts,
the queue fetch statement and error classification.

SQL is written with ``?`` placeholders; ``_exec`` adapts them to the driver.
"""

from __future__ import annotations

import json
import os
import random
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence, TypeVar

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

T = TypeVar("T")

_RECURRING_FIELDS = (
    "invocation", "queue", "cron", "time_zone", "next_execution",
    "last_execution", "last_job_id", "error",
)


def _int_id(job_id: Any) -> Optional[int]:
    try:
        return int(job_id)
    except (TypeError, ValueError):
        return None


def _day(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")


def _hour(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H")


def _marks(n: int) -> str:
    return ",".join("?" * n)


class SQLStorageBase(JobStorage):
    #: True when the driver uses ``%s`` placeholders (PyMySQL) instead of ``?`` (pyodbc).
    format_paramstyle = False
    #: Attempts for a transaction that lost a deadlock.
    max_attempts = 6

    def __init__(self, *, invisibility_timeout: float, job_expiration: float, prepare_schema: bool):
        super().__init__(job_expiration=job_expiration)
        self.invisibility_timeout = invisibility_timeout
        self._local = threading.local()
        if prepare_schema:
            self._prepare_schema()

    # ------------------------------------------------------------------ dialect hooks

    def _connect(self):  # -> DB-API connection in autocommit mode
        raise NotImplementedError

    def _t(self, name: str) -> str:
        """Fully-qualified, quoted table name."""
        raise NotImplementedError

    def _q(self, column: str) -> str:
        """Quoted column name."""
        raise NotImplementedError

    def _begin(self, conn) -> None:
        raise NotImplementedError

    def _end(self, conn, commit: bool) -> None:
        raise NotImplementedError

    def _is_deadlock(self, exc: BaseException) -> bool:
        raise NotImplementedError

    def _is_duplicate(self, exc: BaseException) -> bool:
        raise NotImplementedError

    def _is_disconnect(self, exc: BaseException) -> bool:
        return False

    def _page(self, select: str, order: str, limit: int, offset: int = 0) -> str:
        """``select`` is everything up to (not including) ORDER BY."""
        raise NotImplementedError

    def _locked_select(self, table: str, columns: str, where: str) -> str:
        """SELECT that takes an update lock on the matching rows (inside a transaction)."""
        raise NotImplementedError

    def _insert_returning_id(self, c, table: str, columns: Sequence[str], values: Sequence[Any]) -> int:
        raise NotImplementedError

    def _upsert(self, c, table: str, keys: Dict[str, Any], values: Dict[str, Any],
                insert_only: Optional[Dict[str, Any]] = None) -> None:
        raise NotImplementedError

    def _increment(self, c, counter: str) -> None:
        raise NotImplementedError

    def _fetch(self, queues: List[str], now: float, cutoff: float) -> Optional[dict]:
        """Atomically reserve the next job row: returns {id, job_id, queue} or None."""
        raise NotImplementedError

    def _schema_statements(self) -> List[str]:
        raise NotImplementedError

    # ------------------------------------------------------------------ plumbing

    def _conn(self):
        conn = getattr(self._local, "conn", None)
        if conn is not None and self._local.pid != os.getpid():
            conn = None  # inherited through fork(); never reuse
        if conn is None:
            conn = self._connect()
            self._local.conn = conn
            self._local.pid = os.getpid()
        return conn

    def _drop_connection(self) -> None:
        conn = getattr(self._local, "conn", None)
        self._local.conn = None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def close(self) -> None:
        """Close the calling thread's connection."""
        conn = getattr(self._local, "conn", None)
        if conn is not None and getattr(self._local, "pid", None) == os.getpid():
            self._drop_connection()
        self._local.conn = None

    def _sql(self, sql: str) -> str:
        return sql.replace("%", "%%").replace("?", "%s") if self.format_paramstyle else sql

    def _exec(self, c, sql: str, params: Sequence[Any] = ()) -> None:
        c.execute(self._sql(sql), tuple(params))

    @staticmethod
    def _rows(c) -> List[dict]:
        if c.description is None:
            return []
        names = [d[0] for d in c.description]
        return [dict(zip(names, row)) for row in c.fetchall()]

    def _query(self, sql: str, params: Sequence[Any] = ()) -> List[dict]:
        for attempt in (1, 2):
            conn = self._conn()
            try:
                c = conn.cursor()
                try:
                    self._exec(c, sql, params)
                    return self._rows(c)
                finally:
                    c.close()
            except Exception as e:
                if attempt == 1 and self._is_disconnect(e):
                    self._drop_connection()  # server restarted / idle timeout: reconnect once
                    continue
                raise
        raise AssertionError("unreachable")

    def _tx(self, fn: Callable[[Any], T]) -> T:
        """Run ``fn(cursor)`` in a transaction, retrying when chosen as a deadlock victim."""
        for attempt in range(1, self.max_attempts + 1):
            conn = self._conn()
            try:
                self._begin(conn)
            except Exception as e:
                if attempt == 1 and self._is_disconnect(e):
                    self._drop_connection()
                    continue
                raise
            c = conn.cursor()
            try:
                result = fn(c)
                c.close()
                self._end(conn, commit=True)
                return result
            except Exception as e:
                try:
                    c.close()
                    self._end(conn, commit=False)
                except Exception:
                    self._drop_connection()
                if self._is_deadlock(e) and attempt < self.max_attempts:
                    time.sleep(random.uniform(0.005, 0.05) * attempt)
                    continue
                raise
        raise AssertionError("unreachable")

    def _prepare_schema(self) -> None:
        conn = self._conn()
        c = conn.cursor()
        try:
            for statement in self._schema_statements():
                c.execute(statement)
        finally:
            c.close()

    # ------------------------------------------------------------------ locks

    def _try_acquire_lock(self, resource: str, owner: str, ttl: float) -> bool:
        now = time.time()
        lock = self._t("lock")

        def fn(c):
            self._exec(c, f"DELETE FROM {lock} WHERE resource = ? AND expire_at < ?", (resource, now))
            self._exec(c, f"INSERT INTO {lock} (resource, owner, expire_at) VALUES (?, ?, ?)",
                       (resource, owner, now + ttl))
            return True

        try:
            return self._tx(fn)
        except Exception as e:
            if self._is_duplicate(e):
                return False
            raise

    def _release_lock(self, resource: str, owner: str) -> None:
        self._tx(lambda c: self._exec(c, f"DELETE FROM {self._t('lock')} WHERE resource = ? AND owner = ?",
                                      (resource, owner)))

    # ------------------------------------------------------------------ jobs

    @staticmethod
    def _job_record(row: dict) -> JobRecord:
        return JobRecord(
            id=str(row["id"]),
            invocation=_serialization.loads(row["invocation"]),
            queue=row["queue"],
            state_name=row["state"],
            state_reason=row["state_reason"],
            state_data=_serialization.loads(row["state_data"]) if row["state_data"] else {},
            parameters=_serialization.loads(row["params"]),
            created_at=float(row["created_at"]),
            expire_at=None if row["expire_at"] is None else float(row["expire_at"]),
        )

    def create_job(self, invocation: dict, queue: str, parameters: Dict[str, Any], expire_in: float) -> str:
        now = time.time()
        job_id = self._tx(lambda c: self._insert_returning_id(
            c, self._t("job"), ["invocation", "queue", "params", "created_at", "expire_at"],
            [_serialization.dumps(invocation), queue, _serialization.dumps(parameters), now, now + expire_in]))
        return str(job_id)

    def get_job(self, job_id: str) -> Optional[JobRecord]:
        jid = _int_id(job_id)
        if jid is None:
            return None
        rows = self._query(f"SELECT * FROM {self._t('job')} WHERE id = ?", (jid,))
        return self._job_record(rows[0]) if rows else None

    def set_state(self, job_id, state: State, *, expected_states=None, expire_at=None) -> bool:
        jid = _int_id(job_id)
        if jid is None:
            return False
        now = time.time()
        job, history, queue = self._t("job"), self._t("state_history"), self._t("job_queue")

        def fn(c):
            self._exec(c, self._locked_select(job, "state", "id = ?"), (jid,))
            rows = self._rows(c)
            if not rows:
                return False
            if expected_states is not None and rows[0]["state"] not in expected_states:
                return False
            data = _serialization.dumps(state.serialize())
            enqueue_at = getattr(state, "enqueue_at", None) if state.name == SCHEDULED else None
            self._exec(c, f"UPDATE {job} SET state = ?, state_reason = ?, state_data = ?, enqueue_at = ?, "
                          f"expire_at = ? WHERE id = ?",
                       (state.name, state.reason, data, enqueue_at, expire_at, jid))
            self._exec(c, f"INSERT INTO {history} (job_id, name, reason, data, created_at) VALUES (?, ?, ?, ?, ?)",
                       (jid, state.name, state.reason, data, now))
            if state.name != PROCESSING:
                self._exec(c, f"DELETE FROM {queue} WHERE job_id = ?", (jid,))
            if state.name == ENQUEUED:
                self._exec(c, f"INSERT INTO {queue} (job_id, queue) VALUES (?, ?)", (jid, state.queue))
            if state.name in (SUCCEEDED, FAILED, DELETED):
                name = state.name.lower()
                for key in (f"stats:{name}", f"stats:{name}:{_day(now)}", f"stats:{name}:h:{_hour(now)}"):
                    self._increment(c, key)
            return True

        ok = self._tx(fn)
        if ok and state.name == ENQUEUED:
            self.notify_new_jobs()
        return ok

    def get_job_parameter(self, job_id: str, name: str) -> Any:
        record = self.get_job(job_id)
        return None if record is None else record.parameters.get(name)

    def set_job_parameter(self, job_id: str, name: str, value: Any) -> None:
        jid = _int_id(job_id)
        if jid is None:
            return
        job = self._t("job")

        def fn(c):
            self._exec(c, self._locked_select(job, "params", "id = ?"), (jid,))
            rows = self._rows(c)
            if rows:
                params = _serialization.loads(rows[0]["params"])
                params[name] = value
                self._exec(c, f"UPDATE {job} SET params = ? WHERE id = ?", (_serialization.dumps(params), jid))

        self._tx(fn)

    def get_state_history(self, job_id: str) -> List[StateRecord]:
        jid = _int_id(job_id)
        if jid is None:
            return []
        rows = self._query(f"SELECT name, reason, data, created_at FROM {self._t('state_history')} "
                           f"WHERE job_id = ? ORDER BY id DESC", (jid,))
        return [StateRecord(r["name"], r["reason"], _serialization.loads(r["data"]) if r["data"] else {},
                            float(r["created_at"])) for r in rows]

    # ------------------------------------------------------------------ queues

    def fetch_next(self, queues: Sequence[str]) -> Optional[FetchedJob]:
        if not queues:
            return None
        now = time.time()
        row = self._fetch(list(queues), now, now - self.invisibility_timeout)
        if row is None:
            return None
        return FetchedJob(job_id=str(row["job_id"]), queue=row["queue"], token=int(row["id"]))

    def remove_from_queue(self, fetched: FetchedJob) -> None:
        self._tx(lambda c: self._exec(c, f"DELETE FROM {self._t('job_queue')} WHERE id = ?", (fetched.token,)))

    def heartbeat_fetched(self, tokens: Sequence[Any]) -> None:
        tokens = [int(t) for t in tokens]
        if tokens:
            self._tx(lambda c: self._exec(
                c, f"UPDATE {self._t('job_queue')} SET fetched_at = ? WHERE id IN ({_marks(len(tokens))})",
                (time.time(), *tokens)))

    def get_scheduled_due(self, now: float, limit: int) -> List[str]:
        sql = self._page(f"SELECT id FROM {self._t('job')} WHERE state = ? AND enqueue_at <= ?", "enqueue_at", limit)
        return [str(r["id"]) for r in self._query(sql, (SCHEDULED, now))]

    # ------------------------------------------------------------------ continuations

    def add_continuation(self, parent_id: str, child_id: str) -> None:
        try:
            self._tx(lambda c: self._exec(
                c, f"INSERT INTO {self._t('continuation')} (parent_id, child_id) VALUES (?, ?)",
                (_int_id(parent_id), _int_id(child_id))))
        except Exception as e:
            if not self._is_duplicate(e):
                raise

    def get_continuations(self, parent_id: str) -> List[str]:
        rows = self._query(f"SELECT child_id FROM {self._t('continuation')} WHERE parent_id = ? ORDER BY child_id",
                           (_int_id(parent_id),))
        return [str(r["child_id"]) for r in rows]

    # ------------------------------------------------------------------ recurring jobs

    @staticmethod
    def _recurring_record(row: dict) -> RecurringJobRecord:
        def f(v):
            return None if v is None else float(v)

        return RecurringJobRecord(
            id=row["id"], invocation=_serialization.loads(row["invocation"]), queue=row["queue"],
            cron=row["cron"], time_zone=row["time_zone"], next_execution=f(row["next_execution"]),
            last_execution=f(row["last_execution"]), last_job_id=row["last_job_id"],
            created_at=float(row["created_at"]), error=row["error"],
        )

    def upsert_recurring(self, record: RecurringJobRecord) -> None:
        values = {
            "invocation": _serialization.dumps(record.invocation), "queue": record.queue, "cron": record.cron,
            "time_zone": record.time_zone, "next_execution": record.next_execution,
            "last_execution": record.last_execution, "last_job_id": record.last_job_id, "error": record.error,
        }
        self._tx(lambda c: self._upsert(c, self._t("recurring_job"), {"id": record.id}, values,
                                        insert_only={"created_at": record.created_at}))

    def get_recurring(self, recurring_id: str) -> Optional[RecurringJobRecord]:
        rows = self._query(f"SELECT * FROM {self._t('recurring_job')} WHERE id = ?", (recurring_id,))
        return self._recurring_record(rows[0]) if rows else None

    def get_recurring_jobs(self) -> List[RecurringJobRecord]:
        return [self._recurring_record(r) for r in self._query(f"SELECT * FROM {self._t('recurring_job')} ORDER BY id")]

    def get_due_recurring(self, now: float, limit: int) -> List[RecurringJobRecord]:
        sql = self._page(f"SELECT * FROM {self._t('recurring_job')} WHERE next_execution IS NOT NULL "
                         f"AND next_execution <= ?", "next_execution", limit)
        return [self._recurring_record(r) for r in self._query(sql, (now,))]

    def update_recurring(self, recurring_id: str, **fields: Any) -> None:
        unknown = set(fields) - set(_RECURRING_FIELDS)
        if unknown:
            raise ValueError(f"Unknown recurring job fields: {sorted(unknown)}")
        if not fields:
            return
        if "invocation" in fields:
            fields["invocation"] = _serialization.dumps(fields["invocation"])
        assignments = ", ".join(f"{self._q(name)} = ?" for name in fields)
        self._tx(lambda c: self._exec(c, f"UPDATE {self._t('recurring_job')} SET {assignments} WHERE id = ?",
                                      (*fields.values(), recurring_id)))

    def remove_recurring(self, recurring_id: str) -> bool:
        def fn(c):
            self._exec(c, f"DELETE FROM {self._t('recurring_job')} WHERE id = ?", (recurring_id,))
            return c.rowcount > 0

        return self._tx(fn)

    # ------------------------------------------------------------------ servers

    def _server_record(self, r: dict) -> ServerRecord:
        data = json.loads(r["data"])
        return ServerRecord(
            id=r["id"], name=data["name"], queues=data["queues"], worker_count=data["worker_count"],
            started_at=float(r["started_at"]), heartbeat=float(r["heartbeat"]), requested=data.get("requested"),
        )

    def announce_server(self, server_id: str, name: str, queues: Sequence[str], worker_count: int) -> None:
        now = time.time()
        data = json.dumps({"name": name, "queues": list(queues), "worker_count": worker_count})
        self._tx(lambda c: self._upsert(c, self._t("server"), {"id": server_id},
                                        {"data": data, "heartbeat": now}, insert_only={"started_at": now}))

    def server_heartbeat(self, server_id: str) -> None:
        self._tx(lambda c: self._exec(c, f"UPDATE {self._t('server')} SET heartbeat = ? WHERE id = ?",
                                      (time.time(), server_id)))

    def remove_server(self, server_id: str) -> None:
        self._tx(lambda c: self._exec(c, f"DELETE FROM {self._t('server')} WHERE id = ?", (server_id,)))

    def remove_timed_out_servers(self, timeout: float) -> int:
        def fn(c):
            self._exec(c, f"DELETE FROM {self._t('server')} WHERE heartbeat < ?", (time.time() - timeout,))
            return max(c.rowcount, 0)

        return self._tx(fn)

    def get_servers(self) -> List[ServerRecord]:
        return [self._server_record(r) for r in self._query(f"SELECT * FROM {self._t('server')} ORDER BY started_at")]

    def get_server(self, server_id: str) -> Optional[ServerRecord]:
        rows = self._query(f"SELECT * FROM {self._t('server')} WHERE id = ?", (server_id,))
        return self._server_record(rows[0]) if rows else None

    def _modify_server_data(self, server_id: str, change: Callable[[dict], None]) -> bool:
        table = self._t("server")

        def fn(c):
            self._exec(c, self._locked_select(table, "data", "id = ?"), (server_id,))
            rows = self._rows(c)
            if not rows:
                return False
            data = json.loads(rows[0]["data"])
            change(data)
            self._exec(c, f"UPDATE {table} SET data = ? WHERE id = ?", (json.dumps(data), server_id))
            return True

        return self._tx(fn)

    def request_server_change(self, server_id, worker_count=None, queues=None) -> bool:
        return self._modify_server_data(
            server_id, lambda d: d.__setitem__("requested", merge_request(d.get("requested"), worker_count, queues)))

    def update_server_state(self, server_id, worker_count, queues, applied_request=None) -> None:
        def change(data):
            data.update(worker_count=worker_count, queues=list(queues))
            if applied_request is not None and data.get("requested") == applied_request:
                data.pop("requested", None)

        self._modify_server_data(server_id, change)

    # ------------------------------------------------------------------ maintenance

    def delete_expired_jobs(self, now: float, batch_size: int = 1000) -> int:
        job = self._t("job")

        def fn(c):
            self._exec(c, f"DELETE FROM {self._t('lock')} WHERE expire_at < ?", (now,))
            self._exec(c, f"DELETE FROM {self._t('counter')} WHERE name LIKE ? AND RIGHT(name, 13) < ?",
                       ("stats:%:h:%", _hour(now - 7 * 86400)))
            self._exec(c, self._page(f"SELECT id FROM {job} WHERE expire_at IS NOT NULL AND expire_at < ?",
                                     "expire_at", batch_size), (now,))
            ids = [r["id"] for r in self._rows(c)]
            if not ids:
                return 0
            marks = _marks(len(ids))
            self._exec(c, f"DELETE FROM {self._t('state_history')} WHERE job_id IN ({marks})", ids)
            self._exec(c, f"DELETE FROM {self._t('job_queue')} WHERE job_id IN ({marks})", ids)
            self._exec(c, f"DELETE FROM {self._t('continuation')} WHERE parent_id IN ({marks}) "
                          f"OR child_id IN ({marks})", ids + ids)
            self._exec(c, f"DELETE FROM {job} WHERE id IN ({marks})", ids)
            return len(ids)

        return self._tx(fn)

    # ------------------------------------------------------------------ monitoring

    def _counters(self, names: Sequence[str]) -> Dict[str, int]:
        names = list(names)
        if not names:
            return {}
        rows = self._query(f"SELECT name, value FROM {self._t('counter')} WHERE name IN ({_marks(len(names))})", names)
        return {r["name"]: int(r["value"]) for r in rows}

    def get_statistics(self) -> Dict[str, int]:
        counts = {r["state"]: int(r["n"]) for r in self._query(
            f"SELECT state, COUNT(*) AS n FROM {self._t('job')} WHERE state IS NOT NULL GROUP BY state")}
        stats = {name.lower(): counts.get(name, 0) for name in ALL_STATES}
        totals = self._counters(["stats:succeeded", "stats:deleted"])
        stats["succeeded"] = totals.get("stats:succeeded", 0)
        stats["deleted"] = totals.get("stats:deleted", 0)
        row = self._query(
            f"SELECT (SELECT COUNT(*) FROM {self._t('server')}) AS servers, "
            f"(SELECT COUNT(*) FROM {self._t('recurring_job')}) AS recurring, "
            f"(SELECT COUNT(DISTINCT queue) FROM {self._t('job_queue')}) AS queues")[0]
        stats.update(servers=int(row["servers"]), recurring=int(row["recurring"]), queues=int(row["queues"]))
        return stats

    def get_jobs(self, state: str, offset: int = 0, limit: int = 20) -> List[JobRecord]:
        order = {SCHEDULED: "enqueue_at ASC, id ASC", ENQUEUED: "id ASC"}.get(state, "id DESC")
        sql = self._page(f"SELECT * FROM {self._t('job')} WHERE state = ?", order, limit, offset)
        return [self._job_record(r) for r in self._query(sql, (state,))]

    def count_jobs(self, state: str) -> int:
        return int(self._query(f"SELECT COUNT(*) AS n FROM {self._t('job')} WHERE state = ?", (state,))[0]["n"])

    def get_queues(self) -> List[QueueInfo]:
        rows = self._query(
            f"SELECT queue, SUM(CASE WHEN fetched_at IS NULL THEN 1 ELSE 0 END) AS enqueued, "
            f"SUM(CASE WHEN fetched_at IS NULL THEN 0 ELSE 1 END) AS fetched "
            f"FROM {self._t('job_queue')} GROUP BY queue ORDER BY queue")
        return [QueueInfo(r["queue"], int(r["enqueued"]), int(r["fetched"])) for r in rows]

    def get_daily_history(self, days: int = 7) -> List[Dict[str, Any]]:
        today = datetime.now(timezone.utc).date()
        dates = [(today - timedelta(days=i)).isoformat() for i in range(days - 1, -1, -1)]
        values = self._counters([f"stats:{s}:{d}" for d in dates for s in ("succeeded", "failed")])
        return [{"date": d, "succeeded": values.get(f"stats:succeeded:{d}", 0),
                 "failed": values.get(f"stats:failed:{d}", 0)} for d in dates]

    def get_hourly_history(self, hours: int = 24) -> List[Dict[str, Any]]:
        now = time.time()
        stamps = [_hour(now - i * 3600) for i in range(hours - 1, -1, -1)]
        values = self._counters([f"stats:{s}:h:{hr}" for hr in stamps for s in ("succeeded", "failed")])
        return [{"hour": hr, "succeeded": values.get(f"stats:succeeded:h:{hr}", 0),
                 "failed": values.get(f"stats:failed:h:{hr}", 0)} for hr in stamps]

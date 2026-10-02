"""PostgreSQL job storage (requires ``psycopg`` 3: ``pip install "schedulefire[postgres]"``).

For production and multi-machine deployments. Workers on any number of machines
can share one database:

* jobs are fetched with ``FOR UPDATE SKIP LOCKED``, so workers never block each other;
* ``LISTEN/NOTIFY`` wakes idle workers in every process as soon as a job is enqueued;
* all tables live in their own schema (``schedulefire`` by default), created on startup.
"""

from __future__ import annotations

import json
import logging
import os
import re
import select
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

#: ON CONFLICT, SKIP LOCKED and array_position arrived in PostgreSQL 9.5.
MIN_SERVER_VERSION = 90500

logger = logging.getLogger("schedulefire")

_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS {s};

CREATE TABLE IF NOT EXISTS {s}.job (
    id           BIGSERIAL PRIMARY KEY,
    invocation   TEXT NOT NULL,
    queue        TEXT NOT NULL,
    state        TEXT,
    state_reason TEXT,
    state_data   TEXT,
    params       TEXT NOT NULL DEFAULT '{{}}',
    created_at   DOUBLE PRECISION NOT NULL,
    enqueue_at   DOUBLE PRECISION,
    expire_at    DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS ix_job_state ON {s}.job (state, id);
CREATE INDEX IF NOT EXISTS ix_job_scheduled ON {s}.job (enqueue_at) WHERE state = 'Scheduled';
CREATE INDEX IF NOT EXISTS ix_job_expire ON {s}.job (expire_at) WHERE expire_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS {s}.state_history (
    id         BIGSERIAL PRIMARY KEY,
    job_id     BIGINT NOT NULL,
    name       TEXT NOT NULL,
    reason     TEXT,
    data       TEXT,
    created_at DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_state_history_job ON {s}.state_history (job_id);

CREATE TABLE IF NOT EXISTS {s}.job_queue (
    id         BIGSERIAL PRIMARY KEY,
    job_id     BIGINT NOT NULL,
    queue      TEXT NOT NULL,
    fetched_at DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS ix_job_queue_fetch ON {s}.job_queue (queue, fetched_at, id);
CREATE INDEX IF NOT EXISTS ix_job_queue_job ON {s}.job_queue (job_id);

CREATE TABLE IF NOT EXISTS {s}.continuation (
    parent_id BIGINT NOT NULL,
    child_id  BIGINT NOT NULL,
    PRIMARY KEY (parent_id, child_id)
);
CREATE INDEX IF NOT EXISTS ix_continuation_child ON {s}.continuation (child_id);

CREATE TABLE IF NOT EXISTS {s}.recurring_job (
    id             TEXT PRIMARY KEY,
    invocation     TEXT NOT NULL,
    queue          TEXT NOT NULL,
    cron           TEXT NOT NULL,
    time_zone      TEXT NOT NULL,
    next_execution DOUBLE PRECISION,
    last_execution DOUBLE PRECISION,
    last_job_id    TEXT,
    created_at     DOUBLE PRECISION NOT NULL,
    error          TEXT
);
CREATE INDEX IF NOT EXISTS ix_recurring_next ON {s}.recurring_job (next_execution);

CREATE TABLE IF NOT EXISTS {s}.server (
    id         TEXT PRIMARY KEY,
    data       TEXT NOT NULL,
    started_at DOUBLE PRECISION NOT NULL,
    heartbeat  DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS {s}.lock (
    resource  TEXT PRIMARY KEY,
    owner     TEXT NOT NULL,
    expire_at DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS {s}.counter (
    key   TEXT PRIMARY KEY,
    value BIGINT NOT NULL
);
"""

_RECURRING_FIELDS = {
    "invocation", "queue", "cron", "time_zone", "next_execution",
    "last_execution", "last_job_id", "error",
}
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,40}$")


def _int_id(job_id: Any) -> Optional[int]:
    try:
        return int(job_id)
    except (TypeError, ValueError):
        return None


def _day(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")


def _hour(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H")


class PostgreSQLStorage(JobStorage):
    def __init__(
        self,
        dsn: str,
        *,
        schema: str = "schedulefire",
        invisibility_timeout: float = 30 * 60,
        job_expiration: float = 24 * 3600,
        prepare_schema: bool = True,
        create_database: bool = True,
        use_notify: bool = True,
    ):
        """
        :param dsn: libpq connection string or URL, e.g. ``postgresql://user:pw@host/db``.
        :param schema: schema holding scheduleFire's tables (created if missing).
        :param create_database: create the database if it doesn't exist (needs CREATEDB).
        :param use_notify: wake idle workers across processes with LISTEN/NOTIFY.
        """
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError:
            raise ImportError('PostgreSQLStorage requires psycopg 3: pip install "schedulefire[postgres]"') from None
        super().__init__(job_expiration=job_expiration)
        if not _IDENTIFIER.match(schema):
            raise ValueError(f"Invalid schema name {schema!r}")
        self._psycopg = psycopg
        self._dict_row = dict_row
        self.dsn = dsn
        self.schema = schema
        self.invisibility_timeout = invisibility_timeout
        self.use_notify = use_notify
        self._channel = f"{schema}_jobs"
        self._local = threading.local()
        self._listener: Optional[threading.Thread] = None
        self._listener_lock = threading.Lock()
        self._disposed = threading.Event()
        self._ensure_database(create_database)
        if prepare_schema:
            with self._tx() as c:
                # Serialize concurrent first-time setup from several processes.
                c.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"schedulefire-schema-{schema}",))
                c.execute(_SCHEMA.format(s=schema))

    def _t(self, table: str) -> str:
        return f"{self.schema}.{table}"

    def _ensure_database(self, create: bool) -> None:
        """Connect once: create the database if it's missing, and check the server version."""
        psycopg = self._psycopg
        try:
            conn = self._conn()
        except psycopg.OperationalError as e:
            if not create or "does not exist" not in str(e):
                raise
            from psycopg import sql
            from psycopg.conninfo import conninfo_to_dict, make_conninfo

            name = conninfo_to_dict(self.dsn).get("dbname")
            if not name:
                raise
            try:
                with psycopg.connect(make_conninfo(self.dsn, dbname="postgres"), autocommit=True) as admin:
                    admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
                logger.info("Created PostgreSQL database %r", name)
            except psycopg.errors.DuplicateDatabase:
                pass  # created concurrently by another process
            except psycopg.Error as create_error:
                raise RuntimeError(
                    f"PostgreSQL database {name!r} does not exist and could not be created automatically "
                    f"({create_error}). Create it once with: CREATE DATABASE \"{name}\";"
                ) from e
            conn = self._conn()
        version = conn.info.server_version
        if version < MIN_SERVER_VERSION:
            raise UnsupportedDatabaseVersionError(
                f"PostgreSQLStorage needs PostgreSQL 9.5 or newer; the server is {version // 10000}.{version % 10000 // 100}."
            )

    # -- connections --------------------------------------------------------------

    def _conn(self):
        conn = getattr(self._local, "conn", None)
        if conn is not None and (conn.closed or conn.broken or self._local.pid != os.getpid()):
            conn = None
        if conn is None:
            conn = self._psycopg.connect(self.dsn, autocommit=True, row_factory=self._dict_row)
            self._local.conn = conn
            self._local.pid = os.getpid()
        return conn

    @contextmanager
    def _tx(self) -> Iterator[Any]:
        conn = self._conn()
        with conn.transaction():
            with conn.cursor() as cur:
                yield cur

    def _query(self, sql: str, params: Sequence[Any] = ()) -> List[dict]:
        with self._conn().cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def close(self) -> None:
        """Close the calling thread's connection."""
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            self._local.conn = None
            if self._local.pid == os.getpid():
                conn.close()

    def dispose(self) -> None:
        self._disposed.set()
        self.close()

    # -- cross-process wake-up ----------------------------------------------------

    def wait_for_jobs(self, timeout: float) -> None:
        if self.use_notify:
            self._ensure_listener()
        super().wait_for_jobs(timeout)

    def _ensure_listener(self) -> None:
        with self._listener_lock:
            if self._listener is not None and self._listener.is_alive():
                return
            self._listener = threading.Thread(target=self._listen, name="schedulefire-pg-listener", daemon=True)
            self._listener.start()

    def _listen(self) -> None:
        while not self._disposed.is_set():
            try:
                with self._psycopg.connect(self.dsn, autocommit=True) as conn:
                    # Works on every psycopg 3 version (notifies(timeout=...) needs 3.2+):
                    # wait for the socket to become readable, then let a no-op query
                    # consume the input, which dispatches notifications to the handler.
                    conn.add_notify_handler(lambda _notification: self.notify_new_jobs())
                    conn.execute(f"LISTEN {self._channel}")
                    while not self._disposed.is_set():
                        readable, _, _ = select.select([conn.fileno()], [], [], 1.0)
                        if readable:
                            conn.execute("SELECT 1")
            except Exception:
                if self._disposed.is_set():
                    return  # connection closed by dispose()
                logger.warning("PostgreSQL LISTEN connection lost; retrying in 5s", exc_info=True)
                self._disposed.wait(5)

    # -- locks --------------------------------------------------------------------

    def _try_acquire_lock(self, resource: str, owner: str, ttl: float) -> bool:
        now = time.time()
        rows = self._query(
            f"INSERT INTO {self._t('lock')} (resource, owner, expire_at) VALUES (%s, %s, %s) "
            f"ON CONFLICT (resource) DO UPDATE SET owner = EXCLUDED.owner, expire_at = EXCLUDED.expire_at "
            f"WHERE {self._t('lock')}.expire_at < %s RETURNING owner",
            (resource, owner, now + ttl, now),
        )
        return bool(rows) and rows[0]["owner"] == owner

    def _release_lock(self, resource: str, owner: str) -> None:
        self._query(f"DELETE FROM {self._t('lock')} WHERE resource = %s AND owner = %s RETURNING resource",
                    (resource, owner))

    # -- jobs ---------------------------------------------------------------------

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
            created_at=row["created_at"],
            expire_at=row["expire_at"],
        )

    def create_job(self, invocation: dict, queue: str, parameters: Dict[str, Any], expire_in: float) -> str:
        now = time.time()
        rows = self._query(
            f"INSERT INTO {self._t('job')} (invocation, queue, params, created_at, expire_at) "
            f"VALUES (%s, %s, %s, %s, %s) RETURNING id",
            (_serialization.dumps(invocation), queue, _serialization.dumps(parameters), now, now + expire_in),
        )
        return str(rows[0]["id"])

    def get_job(self, job_id: str) -> Optional[JobRecord]:
        jid = _int_id(job_id)
        if jid is None:
            return None
        rows = self._query(f"SELECT * FROM {self._t('job')} WHERE id = %s", (jid,))
        return self._job_record(rows[0]) if rows else None

    def set_state(self, job_id, state: State, *, expected_states=None, expire_at=None) -> bool:
        jid = _int_id(job_id)
        if jid is None:
            return False
        now = time.time()
        with self._tx() as c:
            c.execute(f"SELECT state FROM {self._t('job')} WHERE id = %s FOR UPDATE", (jid,))
            row = c.fetchone()
            if row is None:
                return False
            if expected_states is not None and row["state"] not in expected_states:
                return False
            data = _serialization.dumps(state.serialize())
            enqueue_at = getattr(state, "enqueue_at", None) if state.name == SCHEDULED else None
            c.execute(
                f"UPDATE {self._t('job')} SET state = %s, state_reason = %s, state_data = %s, enqueue_at = %s, "
                f"expire_at = %s WHERE id = %s",
                (state.name, state.reason, data, enqueue_at, expire_at, jid),
            )
            c.execute(
                f"INSERT INTO {self._t('state_history')} (job_id, name, reason, data, created_at) "
                f"VALUES (%s, %s, %s, %s, %s)",
                (jid, state.name, state.reason, data, now),
            )
            if state.name != PROCESSING:
                c.execute(f"DELETE FROM {self._t('job_queue')} WHERE job_id = %s", (jid,))
            if state.name == ENQUEUED:
                c.execute(f"INSERT INTO {self._t('job_queue')} (job_id, queue) VALUES (%s, %s)", (jid, state.queue))
                if self.use_notify:
                    c.execute("SELECT pg_notify(%s, %s)", (self._channel, state.queue))
            if state.name in (SUCCEEDED, FAILED, DELETED):
                name = state.name.lower()
                for key in (f"stats:{name}", f"stats:{name}:{_day(now)}", f"stats:{name}:h:{_hour(now)}"):
                    c.execute(
                        f"INSERT INTO {self._t('counter')} (key, value) VALUES (%s, 1) "
                        f"ON CONFLICT (key) DO UPDATE SET value = {self._t('counter')}.value + 1",
                        (key,),
                    )
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
        with self._tx() as c:
            c.execute(f"SELECT params FROM {self._t('job')} WHERE id = %s FOR UPDATE", (jid,))
            row = c.fetchone()
            if row is None:
                return
            params = _serialization.loads(row["params"])
            params[name] = value
            c.execute(f"UPDATE {self._t('job')} SET params = %s WHERE id = %s", (_serialization.dumps(params), jid))

    def get_state_history(self, job_id: str) -> List[StateRecord]:
        jid = _int_id(job_id)
        if jid is None:
            return []
        rows = self._query(
            f"SELECT name, reason, data, created_at FROM {self._t('state_history')} WHERE job_id = %s ORDER BY id DESC",
            (jid,),
        )
        return [StateRecord(r["name"], r["reason"], _serialization.loads(r["data"]) if r["data"] else {},
                            r["created_at"]) for r in rows]

    # -- queues -------------------------------------------------------------------

    def fetch_next(self, queues: Sequence[str]) -> Optional[FetchedJob]:
        if not queues:
            return None
        now = time.time()
        queues = list(queues)
        rows = self._query(
            f"UPDATE {self._t('job_queue')} SET fetched_at = %s WHERE id = ("
            f"  SELECT id FROM {self._t('job_queue')}"
            f"  WHERE queue = ANY(%s) AND (fetched_at IS NULL OR fetched_at < %s)"
            f"  ORDER BY array_position(%s::text[], queue), id LIMIT 1 FOR UPDATE SKIP LOCKED"
            f") RETURNING id, job_id, queue",
            (now, queues, now - self.invisibility_timeout, queues),
        )
        if not rows:
            return None
        return FetchedJob(job_id=str(rows[0]["job_id"]), queue=rows[0]["queue"], token=rows[0]["id"])

    def remove_from_queue(self, fetched: FetchedJob) -> None:
        self._query(f"DELETE FROM {self._t('job_queue')} WHERE id = %s RETURNING id", (fetched.token,))

    def heartbeat_fetched(self, tokens: Sequence[Any]) -> None:
        tokens = list(tokens)
        if tokens:
            self._query(f"UPDATE {self._t('job_queue')} SET fetched_at = %s WHERE id = ANY(%s) RETURNING id",
                        (time.time(), tokens))

    def get_scheduled_due(self, now: float, limit: int) -> List[str]:
        rows = self._query(
            f"SELECT id FROM {self._t('job')} WHERE state = %s AND enqueue_at <= %s ORDER BY enqueue_at LIMIT %s",
            (SCHEDULED, now, limit),
        )
        return [str(r["id"]) for r in rows]

    # -- continuations ------------------------------------------------------------

    def add_continuation(self, parent_id: str, child_id: str) -> None:
        self._query(
            f"INSERT INTO {self._t('continuation')} (parent_id, child_id) VALUES (%s, %s) "
            f"ON CONFLICT DO NOTHING RETURNING parent_id",
            (_int_id(parent_id), _int_id(child_id)),
        )

    def get_continuations(self, parent_id: str) -> List[str]:
        rows = self._query(
            f"SELECT child_id FROM {self._t('continuation')} WHERE parent_id = %s ORDER BY child_id",
            (_int_id(parent_id),),
        )
        return [str(r["child_id"]) for r in rows]

    # -- recurring jobs -----------------------------------------------------------

    @staticmethod
    def _recurring_record(row: dict) -> RecurringJobRecord:
        return RecurringJobRecord(
            id=row["id"], invocation=_serialization.loads(row["invocation"]), queue=row["queue"], cron=row["cron"],
            time_zone=row["time_zone"], next_execution=row["next_execution"], last_execution=row["last_execution"],
            last_job_id=row["last_job_id"], created_at=row["created_at"], error=row["error"],
        )

    def upsert_recurring(self, record: RecurringJobRecord) -> None:
        self._query(
            f"""INSERT INTO {self._t('recurring_job')}
                (id, invocation, queue, cron, time_zone, next_execution, last_execution, last_job_id, created_at, error)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                  invocation = EXCLUDED.invocation, queue = EXCLUDED.queue, cron = EXCLUDED.cron,
                  time_zone = EXCLUDED.time_zone, next_execution = EXCLUDED.next_execution,
                  last_execution = EXCLUDED.last_execution, last_job_id = EXCLUDED.last_job_id,
                  error = EXCLUDED.error
                RETURNING id""",
            (record.id, _serialization.dumps(record.invocation), record.queue, record.cron, record.time_zone,
             record.next_execution, record.last_execution, record.last_job_id, record.created_at, record.error),
        )

    def get_recurring(self, recurring_id: str) -> Optional[RecurringJobRecord]:
        rows = self._query(f"SELECT * FROM {self._t('recurring_job')} WHERE id = %s", (recurring_id,))
        return self._recurring_record(rows[0]) if rows else None

    def get_recurring_jobs(self) -> List[RecurringJobRecord]:
        return [self._recurring_record(r) for r in self._query(f"SELECT * FROM {self._t('recurring_job')} ORDER BY id")]

    def get_due_recurring(self, now: float, limit: int) -> List[RecurringJobRecord]:
        rows = self._query(
            f"SELECT * FROM {self._t('recurring_job')} WHERE next_execution IS NOT NULL AND next_execution <= %s "
            f"ORDER BY next_execution LIMIT %s",
            (now, limit),
        )
        return [self._recurring_record(r) for r in rows]

    def update_recurring(self, recurring_id: str, **fields: Any) -> None:
        unknown = set(fields) - _RECURRING_FIELDS
        if unknown:
            raise ValueError(f"Unknown recurring job fields: {sorted(unknown)}")
        if not fields:
            return
        if "invocation" in fields:
            fields["invocation"] = _serialization.dumps(fields["invocation"])
        assignments = ", ".join(f"{name} = %s" for name in fields)
        self._query(f"UPDATE {self._t('recurring_job')} SET {assignments} WHERE id = %s RETURNING id",
                    (*fields.values(), recurring_id))

    def remove_recurring(self, recurring_id: str) -> bool:
        return bool(self._query(f"DELETE FROM {self._t('recurring_job')} WHERE id = %s RETURNING id", (recurring_id,)))

    # -- servers ------------------------------------------------------------------

    def announce_server(self, server_id: str, name: str, queues: Sequence[str], worker_count: int) -> None:
        now = time.time()
        data = json.dumps({"name": name, "queues": list(queues), "worker_count": worker_count})
        self._query(
            f"INSERT INTO {self._t('server')} (id, data, started_at, heartbeat) VALUES (%s, %s, %s, %s) "
            f"ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, heartbeat = EXCLUDED.heartbeat RETURNING id",
            (server_id, data, now, now),
        )

    def server_heartbeat(self, server_id: str) -> None:
        self._query(f"UPDATE {self._t('server')} SET heartbeat = %s WHERE id = %s RETURNING id", (time.time(), server_id))

    def remove_server(self, server_id: str) -> None:
        self._query(f"DELETE FROM {self._t('server')} WHERE id = %s RETURNING id", (server_id,))

    def remove_timed_out_servers(self, timeout: float) -> int:
        return len(self._query(f"DELETE FROM {self._t('server')} WHERE heartbeat < %s RETURNING id",
                               (time.time() - timeout,)))

    def _server_record(self, r) -> ServerRecord:
        data = json.loads(r["data"])
        return ServerRecord(
            id=r["id"], name=data["name"], queues=data["queues"], worker_count=data["worker_count"],
            started_at=r["started_at"], heartbeat=r["heartbeat"], requested=data.get("requested"),
        )

    def get_servers(self) -> List[ServerRecord]:
        return [self._server_record(r) for r in self._query(f"SELECT * FROM {self._t('server')} ORDER BY started_at")]

    def get_server(self, server_id: str) -> Optional[ServerRecord]:
        rows = self._query(f"SELECT * FROM {self._t('server')} WHERE id = %s", (server_id,))
        return self._server_record(rows[0]) if rows else None

    def request_server_change(self, server_id, worker_count=None, queues=None) -> bool:
        with self._tx() as c:
            c.execute(f"SELECT data FROM {self._t('server')} WHERE id = %s FOR UPDATE", (server_id,))
            row = c.fetchone()
            if row is None:
                return False
            data = json.loads(row["data"])
            data["requested"] = merge_request(data.get("requested"), worker_count, queues)
            c.execute(f"UPDATE {self._t('server')} SET data = %s WHERE id = %s", (json.dumps(data), server_id))
            return True

    def update_server_state(self, server_id, worker_count, queues, applied_request=None) -> None:
        with self._tx() as c:
            c.execute(f"SELECT data FROM {self._t('server')} WHERE id = %s FOR UPDATE", (server_id,))
            row = c.fetchone()
            if row is None:
                return
            data = json.loads(row["data"])
            data.update(worker_count=worker_count, queues=list(queues))
            if applied_request is not None and data.get("requested") == applied_request:
                data.pop("requested", None)
            c.execute(f"UPDATE {self._t('server')} SET data = %s WHERE id = %s", (json.dumps(data), server_id))

    # -- maintenance --------------------------------------------------------------

    def delete_expired_jobs(self, now: float, batch_size: int = 1000) -> int:
        with self._tx() as c:
            c.execute(f"DELETE FROM {self._t('lock')} WHERE expire_at < %s", (now,))
            c.execute(f"DELETE FROM {self._t('counter')} WHERE key LIKE %s AND right(key, 13) < %s",
                      ("stats:%:h:%", _hour(now - 7 * 86400)))
            c.execute(
                f"SELECT id FROM {self._t('job')} WHERE expire_at < %s LIMIT %s FOR UPDATE SKIP LOCKED",
                (now, batch_size),
            )
            ids = [r["id"] for r in c.fetchall()]
            if not ids:
                return 0
            c.execute(f"DELETE FROM {self._t('state_history')} WHERE job_id = ANY(%s)", (ids,))
            c.execute(f"DELETE FROM {self._t('job_queue')} WHERE job_id = ANY(%s)", (ids,))
            c.execute(f"DELETE FROM {self._t('continuation')} WHERE parent_id = ANY(%s) OR child_id = ANY(%s)",
                      (ids, ids))
            c.execute(f"DELETE FROM {self._t('job')} WHERE id = ANY(%s)", (ids,))
            return len(ids)

    # -- monitoring ---------------------------------------------------------------

    def _counters(self, keys: Sequence[str]) -> Dict[str, int]:
        rows = self._query(f"SELECT key, value FROM {self._t('counter')} WHERE key = ANY(%s)", (list(keys),))
        return {r["key"]: r["value"] for r in rows}

    def get_statistics(self) -> Dict[str, int]:
        counts = {r["state"]: r["n"] for r in self._query(
            f"SELECT state, COUNT(*) AS n FROM {self._t('job')} WHERE state IS NOT NULL GROUP BY state")}
        stats = {name.lower(): counts.get(name, 0) for name in ALL_STATES}
        totals = self._counters(["stats:succeeded", "stats:deleted"])
        stats["succeeded"] = totals.get("stats:succeeded", 0)
        stats["deleted"] = totals.get("stats:deleted", 0)
        row = self._query(
            f"SELECT (SELECT COUNT(*) FROM {self._t('server')}) AS servers, "
            f"(SELECT COUNT(*) FROM {self._t('recurring_job')}) AS recurring, "
            f"(SELECT COUNT(DISTINCT queue) FROM {self._t('job_queue')}) AS queues")[0]
        stats.update(servers=row["servers"], recurring=row["recurring"], queues=row["queues"])
        return stats

    def get_jobs(self, state: str, offset: int = 0, limit: int = 20) -> List[JobRecord]:
        order = {SCHEDULED: "enqueue_at ASC, id ASC", ENQUEUED: "id ASC"}.get(state, "id DESC")
        rows = self._query(f"SELECT * FROM {self._t('job')} WHERE state = %s ORDER BY {order} LIMIT %s OFFSET %s",
                           (state, limit, offset))
        return [self._job_record(r) for r in rows]

    def count_jobs(self, state: str) -> int:
        return self._query(f"SELECT COUNT(*) AS n FROM {self._t('job')} WHERE state = %s", (state,))[0]["n"]

    def get_queues(self) -> List[QueueInfo]:
        rows = self._query(
            f"SELECT queue, COUNT(*) FILTER (WHERE fetched_at IS NULL) AS enqueued, "
            f"COUNT(*) FILTER (WHERE fetched_at IS NOT NULL) AS fetched "
            f"FROM {self._t('job_queue')} GROUP BY queue ORDER BY queue")
        return [QueueInfo(r["queue"], r["enqueued"], r["fetched"]) for r in rows]

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

"""MySQL / MariaDB job storage (requires PyMySQL: ``pip install "schedulefire[mysql]"``).

Needs MySQL 8.0+ or MariaDB 10.6+ (for ``SKIP LOCKED``). Tables are created on startup
in the connected database, named with a prefix (``schedulefire_job``, ...). Sessions use
READ COMMITTED isolation to avoid InnoDB gap-lock contention between workers.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Sequence
from urllib.parse import parse_qsl, unquote, urlsplit

from ._sqlbase import SQLStorageBase
from .base import UnsupportedDatabaseVersionError, version_tuple

logger = logging.getLogger("schedulefire")

MIN_MYSQL = (5, 7, 0)
MIN_MARIADB = (10, 3, 0)

_PREFIX = re.compile(r"^[A-Za-z0-9_]{0,30}$")
_DEADLOCK_CODES = {1213, 1205}  # deadlock victim, lock wait timeout
_DISCONNECT_CODES = {2006, 2013, 2055, 4031}


def _parse_url(url: str) -> Dict[str, Any]:
    parts = urlsplit(url)
    if parts.scheme not in ("mysql", "mysql+pymysql", "mariadb"):
        raise ValueError(f"Not a MySQL URL: {url!r}")
    params: Dict[str, Any] = {
        "host": parts.hostname or "localhost",
        "port": parts.port or 3306,
        "user": unquote(parts.username) if parts.username else "root",
        "password": unquote(parts.password) if parts.password else "",
        "database": parts.path.lstrip("/") or None,
    }
    for key, value in parse_qsl(parts.query):
        params[key] = int(value) if value.isdigit() else value
    if not params["database"]:
        raise ValueError("The MySQL URL must include a database name: mysql://user:pw@host:3306/dbname")
    return params


class MySQLStorage(SQLStorageBase):
    format_paramstyle = True

    def __init__(
        self,
        url: Optional[str] = None,
        *,
        table_prefix: str = "schedulefire_",
        invisibility_timeout: float = 30 * 60,
        job_expiration: float = 24 * 3600,
        prepare_schema: bool = True,
        create_database: bool = True,
        **connect_kwargs: Any,
    ):
        """
        :param url: ``mysql://user:password@host:3306/database``. Or omit it and pass
            PyMySQL ``connect()`` arguments (host=, user=, password=, database=, ssl=, ...).
        :param table_prefix: prefix for scheduleFire's table names.
        :param create_database: create the database if it doesn't exist (utf8mb4).
        """
        try:
            import pymysql
        except ImportError:
            raise ImportError('MySQLStorage requires PyMySQL: pip install "schedulefire[mysql]"') from None
        if not _PREFIX.match(table_prefix):
            raise ValueError(f"Invalid table prefix {table_prefix!r}")
        self._pymysql = pymysql
        self.table_prefix = table_prefix
        params = _parse_url(url) if url else {}
        params.update(connect_kwargs)
        params.setdefault("charset", "utf8mb4")
        params["autocommit"] = True
        self._connect_params = params
        self._ensure_database(create_database)
        super().__init__(invisibility_timeout=invisibility_timeout, job_expiration=job_expiration,
                         prepare_schema=prepare_schema)

    def _ensure_database(self, create: bool) -> None:
        """Create the database if missing; detect the server version and its features."""
        pymysql = self._pymysql
        try:
            conn = pymysql.connect(**self._connect_params)
        except pymysql.OperationalError as e:
            name = self._connect_params.get("database")
            if not create or not name or self._code(e) != 1049:  # 1049 = unknown database
                raise
            server = {k: v for k, v in self._connect_params.items() if k != "database"}
            try:
                admin = pymysql.connect(**server)
                with admin.cursor() as c:
                    c.execute(f"CREATE DATABASE IF NOT EXISTS `{name.replace('`', '``')}` "
                              f"CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
                admin.close()
                logger.info("Created MySQL database %r", name)
            except pymysql.MySQLError as create_error:
                raise RuntimeError(
                    f"MySQL database {name!r} does not exist and could not be created automatically "
                    f"({create_error}). Create it once with: CREATE DATABASE `{name}` CHARACTER SET utf8mb4;"
                ) from e
            conn = pymysql.connect(**self._connect_params)
        try:
            with conn.cursor() as c:
                c.execute("SELECT VERSION()")
                version_text = c.fetchone()[0]
        finally:
            conn.close()
        self.is_mariadb = "mariadb" in version_text.lower()
        self.server_version = version_tuple(version_text)
        minimum = MIN_MARIADB if self.is_mariadb else MIN_MYSQL
        if self.server_version < minimum:
            product = "MariaDB" if self.is_mariadb else "MySQL"
            raise UnsupportedDatabaseVersionError(
                f"MySQLStorage needs {product} {'.'.join(map(str, minimum))} or newer; the server is {version_text}."
            )
        #: FOR UPDATE SKIP LOCKED: MySQL 8.0.1+, MariaDB 10.6+.
        self.supports_skip_locked = self.server_version >= ((10, 6, 0) if self.is_mariadb else (8, 0, 1))
        #: INSERT ... AS new ON DUPLICATE KEY UPDATE x = new.x (VALUES() is deprecated there).
        self.supports_row_alias = not self.is_mariadb and self.server_version >= (8, 0, 19)

    # -- dialect ------------------------------------------------------------------

    def _connect(self):
        conn = self._pymysql.connect(**self._connect_params)
        with conn.cursor() as c:
            c.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
        return conn

    def _t(self, name: str) -> str:
        return f"`{self.table_prefix}{name}`"

    def _q(self, column: str) -> str:
        return f"`{column}`"

    def _begin(self, conn) -> None:
        conn.begin()

    def _end(self, conn, commit: bool) -> None:
        conn.commit() if commit else conn.rollback()

    @staticmethod
    def _code(exc: BaseException) -> Optional[int]:
        args = getattr(exc, "args", ())
        return args[0] if args and isinstance(args[0], int) else None

    def _is_deadlock(self, exc: BaseException) -> bool:
        return isinstance(exc, self._pymysql.MySQLError) and self._code(exc) in _DEADLOCK_CODES

    def _is_duplicate(self, exc: BaseException) -> bool:
        return isinstance(exc, self._pymysql.IntegrityError) and self._code(exc) == 1062

    def _is_disconnect(self, exc: BaseException) -> bool:
        return isinstance(exc, (self._pymysql.OperationalError, self._pymysql.InterfaceError)) and (
            self._code(exc) in _DISCONNECT_CODES or isinstance(exc, self._pymysql.InterfaceError))

    def _page(self, select: str, order: str, limit: int, offset: int = 0) -> str:
        return f"{select} ORDER BY {order} LIMIT {int(limit)} OFFSET {int(offset)}"

    def _locked_select(self, table: str, columns: str, where: str) -> str:
        return f"SELECT {columns} FROM {table} WHERE {where} FOR UPDATE"

    def _insert_returning_id(self, c, table: str, columns: Sequence[str], values: Sequence[Any]) -> int:
        cols = ", ".join(self._q(col) for col in columns)
        self._exec(c, f"INSERT INTO {table} ({cols}) VALUES ({','.join('?' * len(values))})", values)
        return int(c.lastrowid)

    def _upsert(self, c, table, keys, values, insert_only=None) -> None:
        row = {**keys, **values, **(insert_only or {})}
        cols = ", ".join(self._q(k) for k in row)
        placeholders = ",".join("?" * len(row))
        if self.supports_row_alias:
            updates = ", ".join(f"{self._q(k)} = new_row.{self._q(k)}" for k in values)
            sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) AS new_row ON DUPLICATE KEY UPDATE {updates}"
        else:
            updates = ", ".join(f"{self._q(k)} = VALUES({self._q(k)})" for k in values)
            sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
        self._exec(c, sql, list(row.values()))

    def _increment(self, c, counter: str) -> None:
        self._exec(c, f"INSERT INTO {self._t('counter')} (name, value) VALUES (?, 1) "
                      f"ON DUPLICATE KEY UPDATE value = value + 1", (counter,))

    def _fetch(self, queues: List[str], now: float, cutoff: float) -> Optional[dict]:
        queue = self._t("job_queue")
        priority = "CASE queue " + " ".join(f"WHEN ? THEN {i}" for i in range(len(queues))) + " END"
        select = (f"SELECT id, job_id, queue FROM {queue} WHERE queue IN ({','.join('?' * len(queues))}) "
                  f"AND (fetched_at IS NULL OR fetched_at < ?) ORDER BY {priority}, id LIMIT 1")
        params = [*queues, cutoff, *queues]
        # Cheap non-locking read first, so idle workers don't open write transactions.
        if not self._query(select, params):
            return None
        if not self.supports_skip_locked:
            return self._fetch_optimistic(queues, now, cutoff, select.replace("LIMIT 1", "LIMIT 10"), params)

        def fn(c):
            self._exec(c, select + " FOR UPDATE SKIP LOCKED", params)
            rows = self._rows(c)
            if not rows:
                return None
            self._exec(c, f"UPDATE {queue} SET fetched_at = ? WHERE id = ?", (now, rows[0]["id"]))
            return rows[0]

        return self._tx(fn)

    def _fetch_optimistic(self, queues, now, cutoff, select, params) -> Optional[dict]:
        """For servers without SKIP LOCKED: claim a candidate with a conditional UPDATE.

        The UPDATE re-checks "not fetched yet" on the latest row version, so of several
        workers racing for the same row exactly one sees rowcount == 1.
        """
        queue = self._t("job_queue")
        for _ in range(3):
            candidates = self._query(select, params)
            if not candidates:
                return None
            for row in candidates:
                def claim(c, row=row):
                    self._exec(c, f"UPDATE {queue} SET fetched_at = ? WHERE id = ? "
                                  f"AND (fetched_at IS NULL OR fetched_at < ?)", (now, row["id"], cutoff))
                    return c.rowcount == 1

                if self._tx(claim):
                    return row
        return None

    def _schema_statements(self) -> List[str]:
        t = self._t
        tail = "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
        return [
            f"""CREATE TABLE IF NOT EXISTS {t('job')} (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                invocation LONGTEXT NOT NULL,
                queue VARCHAR(191) NOT NULL,
                state VARCHAR(20) NULL,
                state_reason TEXT NULL,
                state_data LONGTEXT NULL,
                params LONGTEXT NOT NULL,
                created_at DOUBLE NOT NULL,
                enqueue_at DOUBLE NULL,
                expire_at DOUBLE NULL,
                KEY ix_job_state (state, id),
                KEY ix_job_scheduled (state, enqueue_at),
                KEY ix_job_expire (expire_at)
            ) {tail}""",
            f"""CREATE TABLE IF NOT EXISTS {t('state_history')} (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                job_id BIGINT NOT NULL,
                name VARCHAR(20) NOT NULL,
                reason TEXT NULL,
                data LONGTEXT NULL,
                created_at DOUBLE NOT NULL,
                KEY ix_state_history_job (job_id)
            ) {tail}""",
            f"""CREATE TABLE IF NOT EXISTS {t('job_queue')} (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                job_id BIGINT NOT NULL,
                queue VARCHAR(191) NOT NULL,
                fetched_at DOUBLE NULL,
                KEY ix_job_queue_fetch (queue, fetched_at, id),
                KEY ix_job_queue_job (job_id)
            ) {tail}""",
            f"""CREATE TABLE IF NOT EXISTS {t('continuation')} (
                parent_id BIGINT NOT NULL,
                child_id BIGINT NOT NULL,
                PRIMARY KEY (parent_id, child_id),
                KEY ix_continuation_child (child_id)
            ) {tail}""",
            f"""CREATE TABLE IF NOT EXISTS {t('recurring_job')} (
                id VARCHAR(191) NOT NULL PRIMARY KEY,
                invocation LONGTEXT NOT NULL,
                queue VARCHAR(191) NOT NULL,
                cron VARCHAR(255) NOT NULL,
                time_zone VARCHAR(100) NOT NULL,
                next_execution DOUBLE NULL,
                last_execution DOUBLE NULL,
                last_job_id VARCHAR(40) NULL,
                created_at DOUBLE NOT NULL,
                error TEXT NULL,
                KEY ix_recurring_next (next_execution)
            ) {tail}""",
            f"""CREATE TABLE IF NOT EXISTS {t('server')} (
                id VARCHAR(191) NOT NULL PRIMARY KEY,
                data TEXT NOT NULL,
                started_at DOUBLE NOT NULL,
                heartbeat DOUBLE NOT NULL
            ) {tail}""",
            f"""CREATE TABLE IF NOT EXISTS {t('lock')} (
                resource VARCHAR(191) NOT NULL PRIMARY KEY,
                owner VARCHAR(64) NOT NULL,
                expire_at DOUBLE NOT NULL
            ) {tail}""",
            f"""CREATE TABLE IF NOT EXISTS {t('counter')} (
                name VARCHAR(191) NOT NULL PRIMARY KEY,
                value BIGINT NOT NULL
            ) {tail}""",
        ]

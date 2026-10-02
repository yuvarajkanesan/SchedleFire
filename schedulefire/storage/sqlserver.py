"""Microsoft SQL Server / Azure SQL job storage (requires pyodbc and a Microsoft ODBC
driver: ``pip install "schedulefire[sqlserver]"``).

Works with SQL Server 2016+, Azure SQL and LocalDB. Tables live in their own schema
(``schedulefire`` by default), created on startup. Jobs are fetched with
``READPAST, UPDLOCK`` hints - the same technique Hangfire.SqlServer uses - so workers
never block each other.
"""

from __future__ import annotations

import logging
import re
from typing import Any, List, Optional, Sequence
from urllib.parse import parse_qsl, unquote, urlsplit

from ._sqlbase import SQLStorageBase
from .base import UnsupportedDatabaseVersionError

logger = logging.getLogger("schedulefire")

#: OFFSET/FETCH paging needs SQL Server 2012 (version 11). Azure SQL reports 12.
MIN_MAJOR_VERSION = 11
_DATABASE_KEY = re.compile(r"(?i)(^|;)\s*(database|initial catalog)\s*=\s*(\{[^}]*\}|[^;]*)")

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,40}$")
_DEADLOCK_STATES = {"40001"}                      # deadlock victim
_DISCONNECT_STATES = {"08S01", "08001", "08003", "08004", "08007", "01000"}
_BIN = "COLLATE Latin1_General_BIN2"             # case-sensitive ids/queue names


def odbc_driver(pyodbc) -> str:
    """The newest installed 'ODBC Driver NN for SQL Server'."""
    found = []
    for name in pyodbc.drivers():
        m = re.match(r"ODBC Driver (\d+) for SQL Server$", name)
        if m:
            found.append((int(m.group(1)), name))
    if not found:
        raise RuntimeError(
            "No Microsoft ODBC driver for SQL Server found. Install 'ODBC Driver 18 for SQL Server' "
            "(https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)."
        )
    return max(found)[1]


def connection_string_from_url(url: str, pyodbc) -> str:
    """``mssql://user:pw@host:1433/db?Encrypt=yes`` -> ODBC connection string.

    Without a user name, Windows authentication (Trusted_Connection) is used. Extra query
    parameters are passed through as ODBC keywords (e.g. ``TrustServerCertificate=yes``).
    """
    parts = urlsplit(url)
    if parts.scheme not in ("mssql", "sqlserver", "mssql+pyodbc"):
        raise ValueError(f"Not a SQL Server URL: {url!r}")
    database = unquote(parts.path.lstrip("/"))
    if not database:
        raise ValueError("The SQL Server URL must include a database: mssql://user:pw@host:1433/dbname")
    host = unquote(parts.hostname or "localhost")
    server = f"{host},{parts.port}" if parts.port else host
    options = dict(parse_qsl(parts.query))
    items = {"Driver": "{" + options.pop("driver", odbc_driver(pyodbc)) + "}", "Server": server, "Database": database}
    if parts.username:
        items["UID"] = unquote(parts.username)
        items["PWD"] = "{" + unquote(parts.password or "").replace("}", "}}") + "}"
    else:
        items["Trusted_Connection"] = "yes"
    items.update(options)
    return ";".join(f"{k}={v}" for k, v in items.items()) + ";"


class SQLServerStorage(SQLStorageBase):
    def __init__(
        self,
        connection: str,
        *,
        schema: str = "schedulefire",
        invisibility_timeout: float = 30 * 60,
        job_expiration: float = 24 * 3600,
        prepare_schema: bool = True,
        create_database: bool = True,
    ):
        """
        :param create_database: create the database if it doesn't exist (needs CREATE DATABASE permission).
        :param connection: an ODBC connection string, e.g.
            ``"Driver={ODBC Driver 18 for SQL Server};Server=db;Database=app;UID=u;PWD=p;Encrypt=yes"``,
            or a URL: ``mssql://user:pw@host:1433/database`` (Windows auth when no user).
        :param schema: schema holding scheduleFire's tables (created if missing).
        """
        try:
            import pyodbc
        except ImportError:
            raise ImportError('SQLServerStorage requires pyodbc: pip install "schedulefire[sqlserver]"') from None
        if not _IDENTIFIER.match(schema):
            raise ValueError(f"Invalid schema name {schema!r}")
        self._pyodbc = pyodbc
        self.schema = schema
        if connection.lower().startswith(("mssql://", "sqlserver://", "mssql+pyodbc://")):
            connection = connection_string_from_url(connection, pyodbc)
        self.connection_string = connection
        self._ensure_database(create_database)
        super().__init__(invisibility_timeout=invisibility_timeout, job_expiration=job_expiration,
                         prepare_schema=prepare_schema)

    def _ensure_database(self, create: bool) -> None:
        """Create the database if missing (via master) and check the server version."""
        pyodbc = self._pyodbc
        try:
            conn = self._connect()
        except pyodbc.Error as e:
            match = _DATABASE_KEY.search(self.connection_string)
            name = match.group(3).strip().strip("{}") if match else ""
            if not create or not name or "4060" not in str(e):  # 4060 = cannot open database
                raise
            master = _DATABASE_KEY.sub(lambda m: f"{m.group(1)}Database=master", self.connection_string, count=1)
            try:
                admin = pyodbc.connect(master, autocommit=True)
                admin.execute(f"IF DB_ID(?) IS NULL CREATE DATABASE [{name.replace(']', ']]')}]", name)
                admin.close()
                logger.info("Created SQL Server database %r", name)
            except pyodbc.Error as create_error:
                raise RuntimeError(
                    f"SQL Server database {name!r} does not exist and could not be created automatically "
                    f"({create_error}). Create it once with: CREATE DATABASE [{name}];"
                ) from e
            conn = self._connect()
        try:
            version_text = conn.execute("SELECT CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128))").fetchone()[0]
        finally:
            conn.close()
        major = int(str(version_text).split(".")[0])
        if major < MIN_MAJOR_VERSION:
            raise UnsupportedDatabaseVersionError(
                f"SQLServerStorage needs SQL Server 2012 or newer; the server is version {version_text}."
            )

    # -- dialect ------------------------------------------------------------------

    def _connect(self):
        return self._pyodbc.connect(self.connection_string, autocommit=True)

    def _t(self, name: str) -> str:
        return f"[{self.schema}].[{name}]"

    def _q(self, column: str) -> str:
        return f"[{column}]"

    def _begin(self, conn) -> None:
        conn.autocommit = False

    def _end(self, conn, commit: bool) -> None:
        try:
            conn.commit() if commit else conn.rollback()
        finally:
            conn.autocommit = True

    @staticmethod
    def _sqlstate(exc: BaseException) -> str:
        args = getattr(exc, "args", ())
        return str(args[0]) if args else ""

    def _is_deadlock(self, exc: BaseException) -> bool:
        return isinstance(exc, self._pyodbc.Error) and (
            self._sqlstate(exc) in _DEADLOCK_STATES or "(1205)" in str(exc))

    def _is_duplicate(self, exc: BaseException) -> bool:
        return isinstance(exc, self._pyodbc.IntegrityError) and ("(2627)" in str(exc) or "(2601)" in str(exc))

    def _is_disconnect(self, exc: BaseException) -> bool:
        return isinstance(exc, self._pyodbc.Error) and self._sqlstate(exc) in _DISCONNECT_STATES

    def _page(self, select: str, order: str, limit: int, offset: int = 0) -> str:
        return f"{select} ORDER BY {order} OFFSET {int(offset)} ROWS FETCH NEXT {int(limit)} ROWS ONLY"

    def _locked_select(self, table: str, columns: str, where: str) -> str:
        return f"SELECT {columns} FROM {table} WITH (UPDLOCK, ROWLOCK) WHERE {where}"

    def _insert_returning_id(self, c, table: str, columns: Sequence[str], values: Sequence[Any]) -> int:
        cols = ", ".join(self._q(col) for col in columns)
        self._exec(c, f"INSERT INTO {table} ({cols}) OUTPUT inserted.id VALUES ({','.join('?' * len(values))})",
                   values)
        return int(c.fetchone()[0])

    def _upsert(self, c, table, keys, values, insert_only=None) -> None:
        insert = {**keys, **values, **(insert_only or {})}
        match = " AND ".join(f"t.{self._q(k)} = s.{self._q(k)}" for k in keys)
        source = ", ".join(f"? AS {self._q(k)}" for k in keys)
        updates = ", ".join(f"{self._q(k)} = ?" for k in values)
        cols = ", ".join(self._q(k) for k in insert)
        self._exec(
            c,
            f"MERGE {table} WITH (HOLDLOCK) AS t USING (SELECT {source}) AS s ON {match} "
            f"WHEN MATCHED THEN UPDATE SET {updates} "
            f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({','.join('?' * len(insert))});",
            [*keys.values(), *values.values(), *insert.values()],
        )

    def _increment(self, c, counter: str) -> None:
        self._exec(
            c,
            f"MERGE {self._t('counter')} WITH (HOLDLOCK) AS t USING (SELECT ? AS name) AS s ON t.name = s.name "
            f"WHEN MATCHED THEN UPDATE SET value = t.value + 1 "
            f"WHEN NOT MATCHED THEN INSERT (name, value) VALUES (s.name, 1);",
            (counter,),
        )

    def _fetch(self, queues: List[str], now: float, cutoff: float) -> Optional[dict]:
        queue = self._t("job_queue")
        marks = ",".join("?" * len(queues))
        where = f"queue IN ({marks}) AND (fetched_at IS NULL OR fetched_at < ?)"
        # Cheap read without locks first, so idle workers don't take update locks on every poll.
        if not self._query(f"SELECT TOP (1) id FROM {queue} WITH (READPAST) WHERE {where}", [*queues, cutoff]):
            return None
        priority = "CASE queue " + " ".join(f"WHEN ? THEN {i}" for i in range(len(queues))) + " END"
        sql = (f"WITH next_job AS (SELECT TOP (1) id, job_id, queue, fetched_at FROM {queue} "
               f"WITH (ROWLOCK, READPAST, UPDLOCK) WHERE {where} ORDER BY {priority}, id) "
               f"UPDATE next_job SET fetched_at = ? OUTPUT inserted.id, inserted.job_id, inserted.queue;")

        def fn(c):
            self._exec(c, sql, [*queues, cutoff, *queues, now])
            rows = self._rows(c)
            return rows[0] if rows else None

        return self._tx(fn)

    def _prepare_schema(self) -> None:
        # Serialize first-time setup across processes with an application lock.
        def fn(c):
            c.execute("EXEC sp_getapplock @Resource = ?, @LockMode = 'Exclusive', @LockOwner = 'Transaction', "
                      "@LockTimeout = 60000", (f"schedulefire-schema-{self.schema}",))
            for statement in self._schema_statements():
                c.execute(statement)

        self._tx(fn)

    def _schema_statements(self) -> List[str]:
        s = self.schema

        def table(name: str, body: str) -> str:
            return f"IF OBJECT_ID(N'[{s}].[{name}]', N'U') IS NULL CREATE TABLE [{s}].[{name}] ({body})"

        def index(name: str, table_name: str, columns: str) -> str:
            return (f"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'{name}' "
                    f"AND object_id = OBJECT_ID(N'[{s}].[{table_name}]')) "
                    f"CREATE INDEX [{name}] ON [{s}].[{table_name}] ({columns})")

        return [
            f"IF SCHEMA_ID(N'{s}') IS NULL EXEC(N'CREATE SCHEMA [{s}]')",
            table("job", f"""id BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_{s}_job] PRIMARY KEY,
                invocation NVARCHAR(MAX) NOT NULL, queue NVARCHAR(200) {_BIN} NOT NULL,
                state NVARCHAR(20) NULL, state_reason NVARCHAR(MAX) NULL, state_data NVARCHAR(MAX) NULL,
                params NVARCHAR(MAX) NOT NULL, created_at FLOAT NOT NULL, enqueue_at FLOAT NULL, expire_at FLOAT NULL"""),
            index("IX_job_state", "job", "state, id"),
            index("IX_job_scheduled", "job", "state, enqueue_at"),
            index("IX_job_expire", "job", "expire_at"),
            table("state_history", f"""id BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_{s}_state_history] PRIMARY KEY,
                job_id BIGINT NOT NULL, name NVARCHAR(20) NOT NULL, reason NVARCHAR(MAX) NULL,
                data NVARCHAR(MAX) NULL, created_at FLOAT NOT NULL"""),
            index("IX_state_history_job", "state_history", "job_id"),
            table("job_queue", f"""id BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_{s}_job_queue] PRIMARY KEY,
                job_id BIGINT NOT NULL, queue NVARCHAR(200) {_BIN} NOT NULL, fetched_at FLOAT NULL"""),
            index("IX_job_queue_fetch", "job_queue", "queue, fetched_at, id"),
            index("IX_job_queue_job", "job_queue", "job_id"),
            table("continuation", f"""parent_id BIGINT NOT NULL, child_id BIGINT NOT NULL,
                CONSTRAINT [PK_{s}_continuation] PRIMARY KEY (parent_id, child_id)"""),
            index("IX_continuation_child", "continuation", "child_id"),
            table("recurring_job", f"""id NVARCHAR(200) {_BIN} NOT NULL CONSTRAINT [PK_{s}_recurring_job] PRIMARY KEY,
                invocation NVARCHAR(MAX) NOT NULL, queue NVARCHAR(200) {_BIN} NOT NULL, cron NVARCHAR(255) NOT NULL,
                time_zone NVARCHAR(100) NOT NULL, next_execution FLOAT NULL, last_execution FLOAT NULL,
                last_job_id NVARCHAR(40) NULL, created_at FLOAT NOT NULL, error NVARCHAR(MAX) NULL"""),
            index("IX_recurring_next", "recurring_job", "next_execution"),
            table("server", f"""id NVARCHAR(200) {_BIN} NOT NULL CONSTRAINT [PK_{s}_server] PRIMARY KEY,
                data NVARCHAR(MAX) NOT NULL, started_at FLOAT NOT NULL, heartbeat FLOAT NOT NULL"""),
            table("lock", f"""resource NVARCHAR(200) {_BIN} NOT NULL CONSTRAINT [PK_{s}_lock] PRIMARY KEY,
                owner NVARCHAR(64) NOT NULL, expire_at FLOAT NOT NULL"""),
            table("counter", f"""name NVARCHAR(200) {_BIN} NOT NULL CONSTRAINT [PK_{s}_counter] PRIMARY KEY,
                value BIGINT NOT NULL"""),
        ]

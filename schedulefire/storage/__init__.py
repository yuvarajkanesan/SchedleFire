from .base import (
    DistributedLock,
    DistributedLockTimeoutError,
    FetchedJob,
    JobRecord,
    JobStorage,
    QueueInfo,
    RecurringJobRecord,
    ServerRecord,
    StateRecord,
)
from .memory import MemoryStorage
from .mysql import MySQLStorage
from .postgres import PostgreSQLStorage
from .redis import RedisStorage
from .sqlite import SQLiteStorage
from .sqlserver import SQLServerStorage


def storage_from_url(url: str, **options) -> JobStorage:
    """Create a storage from a URL or path.

    * ``postgresql://user:pw@host/db`` (or ``postgres://``) -> :class:`PostgreSQLStorage`
    * ``redis://host:6379/0`` / ``rediss://`` / ``unix://`` -> :class:`RedisStorage`
    * ``mysql://user:pw@host:3306/db`` (or ``mariadb://``) -> :class:`MySQLStorage`
    * ``mssql://user:pw@host:1433/db`` (or ``sqlserver://``), or an ODBC connection string
      starting with ``Driver=`` -> :class:`SQLServerStorage`
    * ``memory://`` -> :class:`MemoryStorage` (single process, nothing persisted)
    * ``sqlite:///path/to/jobs.db`` or a plain file path -> :class:`SQLiteStorage`
    """
    lowered = url.lower()
    if lowered.startswith(("postgresql://", "postgres://")):
        return PostgreSQLStorage(url, **options)
    if lowered.startswith(("redis://", "rediss://", "unix://")):
        return RedisStorage(url, **options)
    if lowered.startswith(("mysql://", "mysql+pymysql://", "mariadb://")):
        return MySQLStorage(url, **options)
    if lowered.startswith(("mssql://", "sqlserver://", "mssql+pyodbc://", "driver=")):
        return SQLServerStorage(url, **options)
    if lowered.startswith("memory://"):
        return MemoryStorage(**options)
    if lowered.startswith("sqlite:///"):
        url = url[len("sqlite:///"):]
    return SQLiteStorage(url, **options)


__all__ = [
    "DistributedLock",
    "DistributedLockTimeoutError",
    "FetchedJob",
    "JobRecord",
    "JobStorage",
    "MemoryStorage",
    "MySQLStorage",
    "PostgreSQLStorage",
    "QueueInfo",
    "RecurringJobRecord",
    "RedisStorage",
    "SQLServerStorage",
    "SQLiteStorage",
    "ServerRecord",
    "StateRecord",
    "storage_from_url",
]

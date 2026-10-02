"""Global configuration - the equivalent of Hangfire's ``GlobalConfiguration``."""

from __future__ import annotations

import logging
import os
import threading
from typing import Optional

from .job import JobActivator, JobSecurityPolicy, set_job_security_policy
from .storage.base import JobStorage

logger = logging.getLogger("schedulefire")

#: Used when no storage is configured and ``SCHEDULEFIRE_DB`` isn't set.
DEFAULT_DATABASE = "schedulefire.db"

_storage: Optional[JobStorage] = None
_activator: JobActivator = JobActivator()
_lock = threading.Lock()


def configure(
    *,
    storage: Optional[JobStorage] = None,
    activator: Optional[JobActivator] = None,
    job_security: Optional[JobSecurityPolicy] = None,
) -> None:
    """Set the default storage/activator used by ``BackgroundJob``, ``RecurringJob`` and servers.

    ::

        schedulefire.configure(
            storage=SQLiteStorage("jobs.db"),
            job_security=JobSecurityPolicy(allowed_modules=["myapp"]),  # only myapp.* functions run as jobs
        )
    """
    global _storage, _activator
    if job_security is not None:
        set_job_security_policy(job_security)
    if storage is not None:
        _storage = storage
    if activator is not None:
        _activator = activator


def get_storage() -> JobStorage:
    """The configured storage. If none was set with ``configure()``, it is created on first use from:

    1. ``SCHEDULEFIRE_DB`` - a storage URL or SQLite path;
    2. the config file (``SCHEDULEFIRE_CONFIG``, or ``schedulefire.toml`` / ``.json`` in the working
       directory) - which also applies its retry policy;
    3. a SQLite file named ``schedulefire.db`` in the working directory.
    """
    global _storage
    if _storage is None:
        with _lock:
            if _storage is None:
                from .settings import find_config_file, load_config
                from .storage import SQLiteStorage, storage_from_url

                path = find_config_file()
                if path:
                    _storage = load_config(path).apply()
                    logger.info("Using storage from config file %s: %s", path, type(_storage).__name__)
                elif os.environ.get("SCHEDULEFIRE_DB"):
                    _storage = storage_from_url(os.environ["SCHEDULEFIRE_DB"])
                    logger.info("Using storage from SCHEDULEFIRE_DB: %s", type(_storage).__name__)
                else:
                    _storage = SQLiteStorage(DEFAULT_DATABASE)
                    logger.warning(
                        "No storage configured; using SQLite at %s. Every process that shares jobs must use "
                        "the same file - call schedulefire.configure(storage=...), set SCHEDULEFIRE_DB, or add "
                        "a schedulefire.toml (run: schedulefire config init).",
                        os.path.abspath(DEFAULT_DATABASE),
                    )
    return _storage


def get_activator() -> JobActivator:
    return _activator

"""scheduleFire - background jobs for Python, modelled on .NET's Hangfire.

::

    import schedulefire
    from schedulefire import BackgroundJob, RecurringJob, Cron, SQLiteStorage, BackgroundJobServer

    schedulefire.configure(storage=SQLiteStorage("jobs.db"))

    BackgroundJob.enqueue(send_email, "a@example.com")                  # fire-and-forget
    BackgroundJob.schedule(send_email, timedelta(hours=1), "a@b.com")   # delayed
    RecurringJob.add_or_update("report", build_report, Cron.daily(9))   # recurring
    BackgroundJob.continue_job_with(job_id, notify_done)                # continuation

    BackgroundJobServer().run()                                          # process jobs
"""

from .client import BackgroundJob, BackgroundJobClient, RecurringJob, RecurringJobManager
from .config import configure, get_activator, get_storage
from .context import JobAbortedError, JobContext, get_job_context
from .cron import Cron, CronExpression
from .filters import (
    AttemptsExceededAction,
    AutomaticRetry,
    DisableConcurrentExecution,
    GlobalJobFilters,
    JobFilter,
)
from .job import Job, JobActivator, JobLoadError, JobSecurityError, JobSecurityPolicy, queue
from .server import BackgroundJobServer, BackgroundProcess, update_server
from .settings import ConfigError, Settings, load_config
from .states import (
    AwaitingState,
    ContinuationOptions,
    DeletedState,
    EnqueuedState,
    FailedState,
    ProcessingState,
    ScheduledState,
    State,
    SucceededState,
)
from .storage import (
    JobStorage,
    MemoryStorage,
    MySQLStorage,
    PostgreSQLStorage,
    RedisStorage,
    SQLiteStorage,
    SQLServerStorage,
    storage_from_url,
)

__version__ = "0.1.0"

__all__ = [
    "AttemptsExceededAction",
    "AutomaticRetry",
    "AwaitingState",
    "BackgroundJob",
    "BackgroundJobClient",
    "BackgroundJobServer",
    "BackgroundProcess",
    "ConfigError",
    "ContinuationOptions",
    "Cron",
    "CronExpression",
    "DeletedState",
    "DisableConcurrentExecution",
    "EnqueuedState",
    "FailedState",
    "GlobalJobFilters",
    "Job",
    "JobAbortedError",
    "JobActivator",
    "JobContext",
    "JobFilter",
    "JobLoadError",
    "JobSecurityError",
    "JobSecurityPolicy",
    "JobStorage",
    "MemoryStorage",
    "MySQLStorage",
    "PostgreSQLStorage",
    "ProcessingState",
    "RecurringJob",
    "RecurringJobManager",
    "RedisStorage",
    "SQLServerStorage",
    "SQLiteStorage",
    "Settings",
    "ScheduledState",
    "State",
    "SucceededState",
    "configure",
    "get_activator",
    "get_job_context",
    "get_storage",
    "load_config",
    "queue",
    "storage_from_url",
    "update_server",
]

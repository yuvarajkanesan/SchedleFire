"""Access to the running job from inside job code (Hangfire's ``PerformContext`` and
``IJobCancellationToken``)."""

from __future__ import annotations

import threading
import time
from contextvars import ContextVar
from typing import Any, Optional

from .job import Job
from .states import PROCESSING
from .storage.base import JobStorage


class JobAbortedError(Exception):
    """Raised by ``throw_if_cancellation_requested`` when the job should stop.

    That happens when the server is shutting down (the job is re-queued) or when the
    job was deleted or re-queued from elsewhere, e.g. the dashboard.
    """


class JobContext:
    #: Seconds between storage checks for "was my job deleted/requeued?".
    state_check_interval = 5.0

    def __init__(
        self,
        job_id: str,
        job: Job,
        storage: JobStorage,
        server_id: str,
        worker_id: str,
        shutdown_event: threading.Event,
    ):
        self.job_id = job_id
        self.job = job
        self.storage = storage
        self.server_id = server_id
        self.worker_id = worker_id
        self._shutdown = shutdown_event
        self._last_check = time.monotonic()
        self._aborted = False

    @property
    def retry_count(self) -> int:
        return int(self.get_job_parameter("RetryCount") or 0)

    def get_job_parameter(self, name: str) -> Any:
        return self.storage.get_job_parameter(self.job_id, name)

    def set_job_parameter(self, name: str, value: Any) -> None:
        self.storage.set_job_parameter(self.job_id, name, value)

    @property
    def is_shutdown_requested(self) -> bool:
        return self._shutdown.is_set()

    @property
    def is_cancellation_requested(self) -> bool:
        if self._shutdown.is_set():
            return True
        now = time.monotonic()
        if not self._aborted and now - self._last_check >= self.state_check_interval:
            self._last_check = now
            record = self.storage.get_job(self.job_id)
            self._aborted = (
                record is None
                or record.state_name != PROCESSING
                or record.state_data.get("server_id") != self.server_id
                or record.state_data.get("worker_id") != self.worker_id
            )
        return self._aborted

    def throw_if_cancellation_requested(self) -> None:
        """Call periodically from long-running jobs to stop cooperatively."""
        if self.is_cancellation_requested:
            raise JobAbortedError(f"Job #{self.job_id} was aborted.")

    def __repr__(self) -> str:
        return f"<JobContext job_id={self.job_id} {self.job.method_name}>"


_current: ContextVar[Optional[JobContext]] = ContextVar("schedulefire_job_context", default=None)


def get_job_context() -> Optional[JobContext]:
    """The context of the job running in this thread, or ``None`` outside a job."""
    return _current.get()

"""Job filters - Hangfire's filter pipeline (client, server, election and apply filters).

A filter can be registered for every job::

    GlobalJobFilters.add(MyLoggingFilter())

or applied to one job function as a decorator, like a Hangfire attribute::

    @AutomaticRetry(attempts=3)
    def import_feed(url): ...

A function-level filter replaces a global filter of the same type (unless the filter
class sets ``allow_multiple = True``).
"""

from __future__ import annotations

import logging
import random
import threading
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Sequence, Tuple, Type, Union

from .job import Job
from .states import DeletedState, FailedState, ScheduledState, State

if TYPE_CHECKING:
    from .context import JobContext
    from .storage.base import JobStorage

logger = logging.getLogger("schedulefire")


@dataclass
class CreateContext:
    storage: "JobStorage"
    job: Job
    initial_state: State
    #: Initial job parameters; filters may add to them in ``on_creating``.
    parameters: Dict[str, Any] = field(default_factory=dict)
    #: Set to True in ``on_creating`` to prevent the job from being created.
    canceled: bool = False
    job_id: Optional[str] = None


@dataclass
class PerformContext:
    storage: "JobStorage"
    job: Job
    job_id: str
    job_context: "JobContext"
    #: Scratch space shared between ``on_performing`` and ``on_performed``.
    items: Dict[Any, Any] = field(default_factory=dict)
    result: Any = None
    exception: Optional[BaseException] = None
    #: Set to True in ``on_performed`` to swallow ``exception``.
    exception_handled: bool = False


@dataclass
class ElectStateContext:
    storage: "JobStorage"
    job_id: str
    job: Optional[Job]
    current_state: Optional[str]
    #: The state about to be applied. Filters may replace it.
    candidate_state: State

    def get_job_parameter(self, name: str) -> Any:
        return self.storage.get_job_parameter(self.job_id, name)

    def set_job_parameter(self, name: str, value: Any) -> None:
        self.storage.set_job_parameter(self.job_id, name, value)


@dataclass
class ApplyStateContext:
    storage: "JobStorage"
    job_id: str
    job: Optional[Job]
    old_state: Optional[str]
    new_state: State


class JobFilter:
    #: Filters run in ascending ``order`` (``on_performed`` / ``on_created`` in reverse).
    order: int = 0
    #: If False, a function-level filter replaces global filters of the same type.
    allow_multiple: bool = False

    def __call__(self, target: Callable) -> Callable:
        """Use the filter as a decorator on a job function, method or class."""
        holder = target.__func__ if isinstance(target, (staticmethod, classmethod)) else target
        filters = list(holder.__dict__.get("__schedulefire_filters__", ()))
        filters.append(self)
        holder.__schedulefire_filters__ = filters
        return target

    # client side
    def on_creating(self, context: CreateContext) -> None: ...
    def on_created(self, context: CreateContext) -> None: ...

    # server side
    def on_performing(self, context: PerformContext) -> None: ...
    def on_performed(self, context: PerformContext) -> None: ...

    # state changes
    def on_state_election(self, context: ElectStateContext) -> None: ...
    def on_state_applied(self, context: ApplyStateContext) -> None: ...


class FilterCollection:
    def __init__(self, filters: Sequence[JobFilter] = ()):
        self._filters: List[JobFilter] = list(filters)
        self._lock = threading.Lock()

    def add(self, job_filter: JobFilter) -> None:
        with self._lock:
            self._filters.append(job_filter)

    def remove(self, job_filter: Union[JobFilter, Type[JobFilter]]) -> None:
        """Remove a filter instance, or every filter of a given type."""
        with self._lock:
            if isinstance(job_filter, type):
                self._filters = [f for f in self._filters if not isinstance(f, job_filter)]
            else:
                self._filters = [f for f in self._filters if f is not job_filter]

    def clear(self) -> None:
        with self._lock:
            self._filters.clear()

    def __iter__(self):
        with self._lock:
            return iter(list(self._filters))

    def __len__(self) -> int:
        return len(self._filters)


def collect_filters(
    func: Any = None,
    owner: Optional[type] = None,
    global_filters: Optional[FilterCollection] = None,
) -> List[JobFilter]:
    """Global filters + class filters + function filters, ordered by ``order``."""
    if global_filters is None:
        global_filters = GlobalJobFilters
    local: List[JobFilter] = [
        *getattr(owner, "__schedulefire_filters__", ()),
        *getattr(func, "__schedulefire_filters__", ()),
    ]
    combined = list(global_filters) + local
    # For single-use filter types the most specific (last) one wins.
    seen: set = set()
    result: List[JobFilter] = []
    for f in reversed(combined):
        if not f.allow_multiple:
            if type(f) in seen:
                continue
            seen.add(type(f))
        result.append(f)
    result.reverse()
    return sorted(result, key=lambda f: f.order)


def filters_for_job(job: Optional[Job], global_filters: Optional[FilterCollection] = None) -> List[JobFilter]:
    func = owner = None
    if job is not None:
        try:
            func, owner = job.load()
        except Exception:  # unloadable target: global filters still apply (e.g. retries)
            pass
    return collect_filters(func, owner, global_filters)


# -- built-in filters -------------------------------------------------------------


class AttemptsExceededAction(str, Enum):
    FAIL = "Fail"
    DELETE = "Delete"


ExceptionTypes = Union[Type[BaseException], Tuple[Type[BaseException], ...]]


class AutomaticRetry(JobFilter):
    """Reschedule failed jobs with an increasing delay (Hangfire's ``[AutomaticRetry]``).

    Registered globally with 10 attempts by default. Use ``@AutomaticRetry(attempts=0)``
    to disable retries for a job.
    """

    order = 20

    def __init__(
        self,
        attempts: int = 10,
        *,
        delays: Optional[Sequence[Union[int, float]]] = None,
        on_attempts_exceeded: AttemptsExceededAction = AttemptsExceededAction.FAIL,
        only_on: Optional[ExceptionTypes] = None,
        except_on: Optional[ExceptionTypes] = None,
    ):
        if attempts < 0:
            raise ValueError("attempts must be >= 0")
        if delays is not None and not delays:
            raise ValueError("delays must not be empty")
        self.attempts = attempts
        self.delays = list(delays) if delays is not None else None
        self.on_attempts_exceeded = AttemptsExceededAction(on_attempts_exceeded)
        self.only_on = only_on
        self.except_on = except_on

    def delay_for(self, retry_attempt: int) -> float:
        """Seconds before retry ``retry_attempt`` (1-based). Same curve as Hangfire."""
        if self.delays is not None:
            return float(self.delays[min(retry_attempt, len(self.delays)) - 1])
        return round((retry_attempt - 1) ** 4 + 15 + random.randint(0, 29) * retry_attempt)

    def on_state_election(self, context: ElectStateContext) -> None:
        failed = context.candidate_state
        if not isinstance(failed, FailedState):
            return
        exc = failed.exception
        from .job import JobSecurityError

        if isinstance(exc, JobSecurityError):
            return  # refused by the security policy: retrying can't help
        if self.only_on is not None and not isinstance(exc, self.only_on):
            return
        if self.except_on is not None and isinstance(exc, self.except_on):
            return

        retry = int(context.get_job_parameter("RetryCount") or 0) + 1
        if retry <= self.attempts:
            delay = self.delay_for(retry)
            context.set_job_parameter("RetryCount", retry)
            context.candidate_state = ScheduledState(
                timedelta(seconds=delay),
                reason=f"Retry attempt {retry} of {self.attempts}: {failed.message}"[:500],
            )
            logger.warning(
                "Job #%s failed (%s). Retry attempt %d of %d in %ss.",
                context.job_id, failed.exception_type, retry, self.attempts, delay,
            )
        elif self.attempts > 0:
            if self.on_attempts_exceeded is AttemptsExceededAction.DELETE:
                context.candidate_state = DeletedState(
                    reason=f"Exceeded the maximum number of retry attempts ({self.attempts})."
                )
            logger.error(
                "Job #%s failed after %d retry attempts (%s: %s).",
                context.job_id, self.attempts, failed.exception_type, failed.message,
            )


class DisableConcurrentExecution(JobFilter):
    """Allow only one instance of the job function to run at a time, across all servers.

    ``timeout``: seconds to wait for the running instance to finish before failing.
    ``ttl``: seconds after which the lock is considered abandoned (crashed worker);
    keep it above the job's longest expected run time.
    """

    order = 30

    def __init__(self, timeout: float = 600, *, ttl: float = 3600, resource: Optional[str] = None):
        self.timeout = timeout
        self.ttl = ttl
        self.resource = resource

    def on_performing(self, context: PerformContext) -> None:
        resource = self.resource or f"job:{context.job.method_name}"
        lock = context.storage.acquire_lock(resource, timeout=self.timeout, ttl=self.ttl)
        context.items[id(self)] = lock

    def on_performed(self, context: PerformContext) -> None:
        lock = context.items.pop(id(self), None)
        if lock is not None:
            lock.release()


#: Filters applied to every job. Retries are on by default, as in Hangfire.
GlobalJobFilters = FilterCollection([AutomaticRetry()])

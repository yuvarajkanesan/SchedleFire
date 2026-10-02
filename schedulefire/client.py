"""Client API: create background jobs and manage recurring jobs.

``BackgroundJob`` and ``RecurringJob`` are static facades over the default storage,
exactly like Hangfire's. Use ``BackgroundJobClient`` / ``RecurringJobManager``
directly when you need a specific storage instance.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any, Callable, Optional, Sequence, Union

from . import config
from .cron import CronExpression
from .filters import CreateContext, FilterCollection, filters_for_job
from .job import Job, validate_queue_name
from .state_machine import StateMachine
from .states import AwaitingState, DeletedState, EnqueuedState, ScheduledState, State
from .storage.base import JobStorage, RecurringJobRecord

logger = logging.getLogger("schedulefire")

JobTarget = Union[Callable, Job]
When = Union[datetime, timedelta, int, float]


def _leading(args: tuple, names: str, method: str):
    """Split ``args`` into the method's own leading arguments and the job's arguments.

    The public methods take ``*args`` instead of named leading parameters so that job
    functions may have keyword arguments called ``target``, ``queue`` or ``when``
    (positional-only ``/`` parameters would do this too, but need Python 3.8).
    """
    count = len(names.split(", "))
    if len(args) < count:
        raise TypeError(f"{method}() missing required positional argument(s): {names}")
    return (*args[:count], args[count:])


def _make_job(target: JobTarget, args: tuple, kwargs: dict) -> Job:
    if isinstance(target, Job):
        if args or kwargs:
            raise TypeError("Pass arguments inside the Job(...) when giving a Job instance.")
        return target
    return Job(target, args, kwargs)


class BackgroundJobClient:
    def __init__(self, storage: Optional[JobStorage] = None, *, global_filters: Optional[FilterCollection] = None):
        self._storage = storage
        self.global_filters = global_filters

    @property
    def storage(self) -> JobStorage:
        return self._storage or config.get_storage()

    def create(self, job: Job, state: State) -> Optional[str]:
        """Create a job in the given initial state. Returns the job id, or None if a filter canceled it."""
        storage = self.storage
        filters = filters_for_job(job, self.global_filters)
        context = CreateContext(storage, job, state)
        for f in filters:
            f.on_creating(context)
            if context.canceled:
                logger.info("Creation of %s was canceled by %s", job.method_name, type(f).__name__)
                return None
        job_id = storage.create_job(
            job.to_invocation(), job.queue, context.parameters, expire_in=storage.job_expiration
        )
        StateMachine(storage, self.global_filters).change_state(job_id, context.initial_state)
        context.job_id = job_id
        for f in reversed(filters):
            f.on_created(context)
        return job_id

    def enqueue(*args: Any, **kwargs: Any) -> str:
        """``enqueue(target, *args, **kwargs)`` - run as soon as a worker is free."""
        self, target, args = _leading(args, "self, target", "enqueue")
        job = _make_job(target, args, kwargs)
        return self.create(job, EnqueuedState(queue=job.queue))

    def enqueue_to(*args: Any, **kwargs: Any) -> str:
        """``enqueue_to(queue, target, *args, **kwargs)``"""
        self, queue, target, args = _leading(args, "self, queue, target", "enqueue_to")
        job = _make_job(target, args, kwargs)
        job.queue = validate_queue_name(queue)
        return self.create(job, EnqueuedState(queue=job.queue))

    def schedule(*args: Any, **kwargs: Any) -> str:
        """``schedule(target, when, *args, **kwargs)`` - ``when`` is a delay (timedelta or
        seconds) or an aware datetime."""
        self, target, when, args = _leading(args, "self, target, when", "schedule")
        return self.create(_make_job(target, args, kwargs), ScheduledState(when))

    def continue_job_with(*args: Any, **kwargs: Any) -> str:
        """``continue_job_with(parent_id, target, *args, **kwargs)`` - run after job ``parent_id``
        succeeds. For other options create with ``AwaitingState``."""
        self, parent_id, target, args = _leading(args, "self, parent_id, target", "continue_job_with")
        return self.create(_make_job(target, args, kwargs), AwaitingState(parent_id))

    def change_state(self, job_id: str, state: State, expected_states: Optional[Sequence[str]] = None) -> bool:
        return StateMachine(self.storage, self.global_filters).change_state(
            str(job_id), state, expected_states=expected_states
        )

    def delete(self, job_id: str, from_state: Optional[str] = None) -> bool:
        return self.change_state(
            job_id, DeletedState(reason="Deleted via API"), [from_state] if from_state else None
        )

    def requeue(self, job_id: str, from_state: Optional[str] = None) -> bool:
        return self.change_state(
            job_id, EnqueuedState(reason="Requeued via API"), [from_state] if from_state else None
        )

    def reschedule(self, job_id: str, when: When, from_state: Optional[str] = None) -> bool:
        return self.change_state(
            job_id, ScheduledState(when, reason="Rescheduled via API"), [from_state] if from_state else None
        )

    def get_parameter(self, job_id: str, name: str) -> Any:
        """Read custom metadata attached to a job (Hangfire's ``GetJobParameter``)."""
        return self.storage.get_job_parameter(job_id, name)

    def set_parameter(self, job_id: str, name: str, value: Any) -> None:
        """Attach custom metadata to a job, from outside the job itself
        (Hangfire's ``SetJobParameter``). From inside a running job, use
        ``get_job_context().set_job_parameter`` instead."""
        self.storage.set_job_parameter(job_id, name, value)


def _default_client() -> BackgroundJobClient:
    return BackgroundJobClient()


class BackgroundJob:
    """Static API for background jobs, mirroring Hangfire's ``BackgroundJob`` class."""

    @staticmethod
    def enqueue(*args: Any, **kwargs: Any) -> str:
        """``enqueue(target, *args, **kwargs)`` - fire-and-forget: run as soon as a worker is free."""
        return _default_client().enqueue(*args, **kwargs)

    @staticmethod
    def enqueue_to(*args: Any, **kwargs: Any) -> str:
        """``enqueue_to(queue, target, *args, **kwargs)``"""
        return _default_client().enqueue_to(*args, **kwargs)

    @staticmethod
    def schedule(*args: Any, **kwargs: Any) -> str:
        """``schedule(target, when, *args, **kwargs)`` - delayed job: ``when`` is a timedelta,
        seconds, or an aware datetime."""
        return _default_client().schedule(*args, **kwargs)

    @staticmethod
    def continue_job_with(*args: Any, **kwargs: Any) -> str:
        """``continue_job_with(parent_id, target, *args, **kwargs)`` - run after ``parent_id`` succeeds."""
        return _default_client().continue_job_with(*args, **kwargs)

    @staticmethod
    def create(job: Job, state: State) -> Optional[str]:
        return _default_client().create(job, state)

    @staticmethod
    def delete(job_id: str, from_state: Optional[str] = None) -> bool:
        return _default_client().delete(job_id, from_state)

    @staticmethod
    def requeue(job_id: str, from_state: Optional[str] = None) -> bool:
        return _default_client().requeue(job_id, from_state)

    @staticmethod
    def reschedule(job_id: str, when: When, from_state: Optional[str] = None) -> bool:
        return _default_client().reschedule(job_id, when, from_state)

    @staticmethod
    def get_parameter(job_id: str, name: str) -> Any:
        return _default_client().get_parameter(job_id, name)

    @staticmethod
    def set_parameter(job_id: str, name: str, value: Any) -> None:
        _default_client().set_parameter(job_id, name, value)


# -- recurring jobs -----------------------------------------------------------------


def time_zone_name(tz: Union[None, str, tzinfo]) -> str:
    if tz is None or tz is timezone.utc:
        return "UTC"
    if isinstance(tz, str):
        get_time_zone(tz)  # validate
        return tz
    key = getattr(tz, "key", None)  # zoneinfo.ZoneInfo
    if key:
        return key
    raise TypeError("time_zone must be None, an IANA name like 'Europe/Paris', or a zoneinfo.ZoneInfo.")


def get_time_zone(name: str) -> tzinfo:
    if name.upper() == "UTC":
        return timezone.utc
    try:
        from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    except ImportError:  # Python 3.7 / 3.8
        try:
            from backports.zoneinfo import ZoneInfo, ZoneInfoNotFoundError  # type: ignore[no-redef]
        except ImportError:
            raise ValueError(
                f"Time zone {name!r} needs zoneinfo, which Python < 3.9 lacks: "
                "pip install backports.zoneinfo tzdata."
            ) from None

    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        raise ValueError(
            f"Unknown time zone {name!r}. On Windows install time zone data with `pip install tzdata`."
        ) from None


def next_execution(cron: str, time_zone: str, after: Optional[datetime] = None) -> Optional[float]:
    after = after or datetime.now(timezone.utc)
    nxt = CronExpression(cron).next_occurrence(after, get_time_zone(time_zone))
    return None if nxt is None else nxt.timestamp()


class RecurringJobManager:
    def __init__(self, storage: Optional[JobStorage] = None, *, global_filters: Optional[FilterCollection] = None):
        self._storage = storage
        self.global_filters = global_filters

    @property
    def storage(self) -> JobStorage:
        return self._storage or config.get_storage()

    def add_or_update(
        self,
        recurring_job_id: str,
        target: JobTarget,
        cron: str,
        *,
        time_zone: Union[None, str, tzinfo] = None,
        queue: Optional[str] = None,
    ) -> None:
        """Create or update a recurring job. Pass ``Job(func, args=...)`` for arguments."""
        if not recurring_job_id or not isinstance(recurring_job_id, str):
            raise ValueError("recurring_job_id must be a non-empty string")
        job = _make_job(target, (), {})
        if queue is not None:
            job.queue = validate_queue_name(queue)
        cron = str(cron).strip()
        CronExpression(cron)  # validate
        tz_name = time_zone_name(time_zone)

        existing = self.storage.get_recurring(recurring_job_id)
        if existing is not None and existing.cron == cron and existing.time_zone == tz_name:
            nxt = existing.next_execution
        else:
            nxt = next_execution(cron, tz_name)
        self.storage.upsert_recurring(RecurringJobRecord(
            id=recurring_job_id,
            invocation=job.to_invocation(),
            queue=job.queue,
            cron=cron,
            time_zone=tz_name,
            next_execution=nxt,
            last_execution=existing.last_execution if existing else None,
            last_job_id=existing.last_job_id if existing else None,
            created_at=existing.created_at if existing else time.time(),
        ))

    def trigger(self, recurring_job_id: str) -> Optional[str]:
        """Run a recurring job now, without changing its schedule. Returns the job id."""
        record = self.storage.get_recurring(recurring_job_id)
        if record is None:
            return None
        job_id = self._create(record, "Triggered using recurring job manager")
        self.storage.update_recurring(record.id, last_execution=time.time(), last_job_id=job_id)
        return job_id

    def remove_if_exists(self, recurring_job_id: str) -> bool:
        return self.storage.remove_recurring(recurring_job_id)

    def _create(self, record: RecurringJobRecord, reason: str) -> Optional[str]:
        client = BackgroundJobClient(self.storage, global_filters=self.global_filters)
        return client.create(record.job, EnqueuedState(queue=record.queue, reason=reason))

    def schedule_due_jobs(self, now: Optional[datetime] = None, limit: int = 1000) -> int:
        """Enqueue every recurring job whose time has come. Called by the server."""
        now = now or datetime.now(timezone.utc)
        count = 0
        for record in self.storage.get_due_recurring(now.timestamp(), limit):
            try:
                nxt = next_execution(record.cron, record.time_zone, now)
            except Exception as e:
                logger.exception("Recurring job '%s' has an invalid schedule", record.id)
                self.storage.update_recurring(record.id, next_execution=None, error=f"{type(e).__name__}: {e}")
                continue
            try:
                job_id = self._create(record, "Triggered by recurring job scheduler")
                self.storage.update_recurring(
                    record.id, next_execution=nxt, last_execution=now.timestamp(), last_job_id=job_id, error=None
                )
                count += 1
            except Exception as e:
                logger.exception("Failed to trigger recurring job '%s'", record.id)
                self.storage.update_recurring(record.id, next_execution=nxt, error=f"{type(e).__name__}: {e}")
        return count


class RecurringJob:
    """Static API for recurring jobs, mirroring Hangfire's ``RecurringJob`` class."""

    @staticmethod
    def add_or_update(
        recurring_job_id: str,
        target: JobTarget,
        cron: str,
        *,
        time_zone: Union[None, str, tzinfo] = None,
        queue: Optional[str] = None,
    ) -> None:
        RecurringJobManager().add_or_update(recurring_job_id, target, cron, time_zone=time_zone, queue=queue)

    @staticmethod
    def trigger(recurring_job_id: str) -> Optional[str]:
        return RecurringJobManager().trigger(recurring_job_id)

    @staticmethod
    def remove_if_exists(recurring_job_id: str) -> bool:
        return RecurringJobManager().remove_if_exists(recurring_job_id)

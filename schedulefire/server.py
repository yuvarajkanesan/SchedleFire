"""BackgroundJobServer - processes jobs, like Hangfire's ``BackgroundJobServer``.

A server runs, as threads:

* ``worker_count`` workers that fetch and perform jobs from ``queues``;
* the delayed-job scheduler (Scheduled -> Enqueued) and recurring-job scheduler;
* a heartbeat (keeps the server and its in-flight jobs alive), a watchdog removing
  dead servers, and the expiration manager deleting old finished jobs.

Any number of servers (threads in one process or separate processes) can share the
same storage; schedulers coordinate through distributed locks.
"""

from __future__ import annotations

import itertools
import logging
import os
import signal
import socket
import threading
import time
import uuid
from typing import Callable, List, Optional, Sequence, Set

from . import config
from .client import RecurringJobManager
from .context import JobAbortedError, JobContext, _current
from .filters import FilterCollection, PerformContext, collect_filters
from .job import JobActivator, JobSecurityError, validate_queue_name
from .state_machine import StateMachine
from .states import (
    ENQUEUED,
    PROCESSING,
    SCHEDULED,
    EnqueuedState,
    FailedState,
    ProcessingState,
    SucceededState,
)
from .storage.base import FetchedJob, JobRecord, JobStorage

logger = logging.getLogger("schedulefire")

_ABORTED = object()  # job's state was changed elsewhere while it ran


class BackgroundProcess:
    """Subclass to run a custom recurring task inside a :class:`BackgroundJobServer`,
    like Hangfire's ``IBackgroundProcess``. ``execute`` is called every ``interval``
    seconds until the server stops; exceptions are logged and don't stop the loop.

    ::

        class ReindexSearch(BackgroundProcess):
            interval = 300.0

            def execute(self, storage):
                ...

        BackgroundJobServer(background_processes=[ReindexSearch()])
    """

    #: Seconds between calls to ``execute``.
    interval: float = 30.0
    #: Thread name; defaults to the class name.
    name: str = ""

    def execute(self, storage: JobStorage) -> None:
        raise NotImplementedError


def _run_pipeline(filters, context: PerformContext, invoke: Callable):
    executed = []
    try:
        for f in filters:
            f.on_performing(context)
            executed.append(f)
        context.result = invoke()
    except BaseException as e:
        context.exception = e
    for f in reversed(executed):
        try:
            f.on_performed(context)
        except Exception as e:
            logger.exception("Filter %s.on_performed failed", type(f).__name__)
            context.exception, context.exception_handled = e, False
    if context.exception is not None and not context.exception_handled:
        raise context.exception
    return context.result


MAX_WORKERS = 1000


def _validate_worker_count(count) -> int:
    try:
        count = int(count)
    except (TypeError, ValueError):
        raise ValueError(f"worker_count must be an integer, got {count!r}") from None
    if not 1 <= count <= MAX_WORKERS:
        raise ValueError(f"worker_count must be between 1 and {MAX_WORKERS}, got {count}")
    return count


def _validate_queues(queues: Sequence[str]) -> List[str]:
    if isinstance(queues, str):
        queues = [q.strip() for q in queues.split(",")]
    result = list(dict.fromkeys(validate_queue_name(q) for q in queues if q))
    if not result:
        raise ValueError("At least one queue is required")
    return result


def update_server(server_id: str, *, worker_count: Optional[int] = None,
                  queues: Optional[Sequence[str]] = None, storage: Optional[JobStorage] = None) -> bool:
    """Change a running server's workers/queues from any process (the server applies it
    within ``control_poll_interval``). Returns False if no such server is registered."""
    if worker_count is None and queues is None:
        raise ValueError("Pass worker_count and/or queues")
    worker_count = None if worker_count is None else _validate_worker_count(worker_count)
    queues = None if queues is None else _validate_queues(queues)
    return (storage or config.get_storage()).request_server_change(server_id, worker_count, queues)


class _Loop(threading.Thread):
    """Runs ``func`` every ``interval`` seconds until the server stops."""

    def __init__(self, name: str, func: Callable[[], None], interval: float, server: "BackgroundJobServer"):
        super().__init__(name=name, daemon=True)
        self.func = func
        self.interval = interval
        self.server = server

    def run(self) -> None:
        stop = self.server._stop
        try:
            while not stop.is_set():
                try:
                    self.func()
                except Exception:
                    logger.exception("Background process '%s' failed", self.name)
                stop.wait(self.interval)
        finally:
            self.server.storage.close()  # this thread's connection


class Worker(threading.Thread):
    def __init__(self, server: "BackgroundJobServer", index: int):
        super().__init__(name=f"schedulefire-worker-{index}", daemon=True)
        self.server = server
        self.worker_id = f"{server.server_id}:worker-{index}"
        #: Set when the server scales down: the worker exits after its current job.
        self.retiring = threading.Event()

    def run(self) -> None:
        try:
            self._loop()
        finally:
            self.server.storage.close()  # this thread's connection

    def _loop(self) -> None:
        server = self.server
        while not server._stop.is_set() and not self.retiring.is_set():
            try:
                fetched = server.storage.fetch_next(server.queues)
            except Exception:
                logger.exception("Worker %s failed to fetch a job", self.worker_id)
                server._stop.wait(server.queue_poll_interval)
                continue
            if fetched is None:
                server.storage.wait_for_jobs(server.queue_poll_interval)
                continue
            try:
                self.process(fetched)
            except Exception:
                # The job stays reserved and is picked up again after the invisibility timeout.
                logger.exception("Worker %s failed while processing job #%s", self.worker_id, fetched.job_id)

    def process(self, fetched: FetchedJob) -> None:
        server = self.server
        storage = server.storage
        machine = StateMachine(storage, server.global_filters)
        record = storage.get_job(fetched.job_id)
        if record is None or record.state_name not in (ENQUEUED, PROCESSING):
            storage.remove_from_queue(fetched)
            return
        if not machine.change_state(
            fetched.job_id,
            ProcessingState(server.server_id, self.worker_id),
            expected_states=[ENQUEUED, PROCESSING],
        ):
            storage.remove_from_queue(fetched)
            return

        server._in_flight_add(fetched.token)
        try:
            outcome = self.perform(record)
            if outcome is None:
                machine.change_state(
                    fetched.job_id,
                    EnqueuedState(reason="Server is shutting down; job re-queued."),
                    expected_states=[PROCESSING],
                )
            elif outcome is not _ABORTED:
                machine.change_state(fetched.job_id, outcome, expected_states=[PROCESSING])
        finally:
            server._in_flight_remove(fetched.token)

    def perform(self, record: JobRecord):
        """Run the job; return the next state, None to re-queue (shutdown), or _ABORTED."""
        server = self.server
        job = record.job
        context = JobContext(record.id, job, server.storage, server.server_id, self.worker_id, server._stop)
        token = _current.set(context)
        started = time.time()
        try:
            func, owner = job.load()
            filters = collect_filters(func, owner, server.global_filters)
            perform_context = PerformContext(server.storage, job, record.id, context)
            logger.debug("Performing job #%s %s", record.id, job.method_name)
            result = _run_pipeline(filters, perform_context, lambda: job.invoke(server.activator))
            return SucceededState(result, latency=started - record.created_at, duration=time.time() - started)
        except JobAbortedError:
            if server._stop.is_set():
                return None
            logger.info("Job #%s was aborted because its state changed.", record.id)
            return _ABORTED
        except JobSecurityError as e:
            logger.warning("SECURITY: refused to run job #%s: %s", record.id, e)
            return FailedState(e, reason="Blocked by the job security policy.")
        except BaseException as e:  # noqa: BLE001 - job code may raise anything, even SystemExit
            logger.debug("Job #%s failed", record.id, exc_info=True)
            return FailedState(e)
        finally:
            _current.reset(token)


class BackgroundJobServer:
    def __init__(
        self,
        storage: Optional[JobStorage] = None,
        *,
        queues: Sequence[str] = ("default",),
        worker_count: Optional[int] = None,
        server_name: Optional[str] = None,
        activator: Optional[JobActivator] = None,
        global_filters: Optional[FilterCollection] = None,
        queue_poll_interval: float = 1.0,
        schedule_polling_interval: float = 15.0,
        heartbeat_interval: float = 30.0,
        server_timeout: float = 5 * 60,
        server_check_interval: float = 60.0,
        expiration_check_interval: float = 30 * 60,
        shutdown_timeout: float = 15.0,
        control_poll_interval: float = 5.0,
        background_processes: Sequence[BackgroundProcess] = (),
    ):
        """
        :param queues: queues to process, highest priority first.
        :param worker_count: defaults to ``min(cpu_count * 5, 20)`` like Hangfire. Workers
            are threads - ideal for I/O-bound jobs. For CPU-bound work run several
            server processes instead.
        :param control_poll_interval: how often the server checks for worker/queue changes
            requested from the dashboard or another process.
        :param background_processes: custom :class:`BackgroundProcess` instances to run
            alongside the built-in schedulers, workers and heartbeat.

        Workers and queues can be changed while the server runs: call
        ``set_worker_count`` / ``set_queues``, use the dashboard's Servers page, or call
        ``schedulefire.update_server(server_id, ...)`` from any process.
        """
        self.storage = storage or config.get_storage()
        self.queues = _validate_queues(queues)
        self.worker_count = _validate_worker_count(worker_count or min((os.cpu_count() or 1) * 5, 20))
        self.server_name = server_name or f"{socket.gethostname()}:{os.getpid()}"
        self.server_id = f"{self.server_name}:{uuid.uuid4().hex[:8]}"
        self.activator = activator or config.get_activator()
        self.global_filters = global_filters
        self.queue_poll_interval = queue_poll_interval
        self.schedule_polling_interval = schedule_polling_interval
        self.heartbeat_interval = heartbeat_interval
        self.server_timeout = server_timeout
        self.server_check_interval = server_check_interval
        self.expiration_check_interval = expiration_check_interval
        self.shutdown_timeout = shutdown_timeout
        self.control_poll_interval = control_poll_interval
        self.background_processes = list(background_processes)

        self._stop = threading.Event()
        self._threads: List[threading.Thread] = []
        self._workers: List[Worker] = []
        self._worker_numbers = itertools.count(1)
        self._scale_lock = threading.RLock()
        self._in_flight: Set = set()
        self._in_flight_lock = threading.Lock()
        self._started = False
        self._stopped = False

    # -- lifecycle ------------------------------------------------------------------

    def start(self) -> "BackgroundJobServer":
        if self._started:
            raise RuntimeError("Server already started")
        self._started = True
        self.storage.announce_server(self.server_id, self.server_name, self.queues, self.worker_count)
        self._threads = [
            _Loop("schedulefire-delayed-scheduler", self._enqueue_scheduled_jobs, self.schedule_polling_interval, self),
            _Loop("schedulefire-recurring-scheduler", self._schedule_recurring_jobs, self.schedule_polling_interval, self),
            _Loop("schedulefire-heartbeat", self._heartbeat, self.heartbeat_interval, self),
            _Loop("schedulefire-watchdog", self._remove_dead_servers, self.server_check_interval, self),
            _Loop("schedulefire-expiration", self._delete_expired_jobs, self.expiration_check_interval, self),
            _Loop("schedulefire-control", self._apply_requested_changes, self.control_poll_interval, self),
        ]
        for i, process in enumerate(self.background_processes):
            name = process.name or f"{type(process).__name__}-{i}"
            self._threads.append(_Loop(name, lambda p=process: p.execute(self.storage), process.interval, self))
        for t in self._threads:
            t.start()
        self._start_workers(self.worker_count)
        logger.info(
            "Server %s started: %d workers, queues %s", self.server_id, self.worker_count, ", ".join(self.queues)
        )
        return self

    def stop(self, wait: bool = True) -> None:
        """Signal shutdown. Running jobs see ``is_cancellation_requested``; jobs still
        running after ``shutdown_timeout`` are recovered later by another server."""
        if not self._started or self._stopped:
            return
        self._stopped = True
        logger.info("Server %s is stopping...", self.server_id)
        self._stop.set()
        self.storage.notify_new_jobs()  # wake idle workers
        if wait:
            deadline = time.monotonic() + self.shutdown_timeout
            with self._scale_lock:
                threads = list(self._threads)
            for t in threads:
                t.join(max(0.0, deadline - time.monotonic()))
            still_running = [t.name for t in threads if t.is_alive()]
            if still_running:
                logger.warning("Threads still running after shutdown timeout: %s", ", ".join(still_running))
        try:
            self.storage.remove_server(self.server_id)
        except Exception:
            logger.exception("Failed to unregister server %s", self.server_id)
        logger.info("Server %s stopped.", self.server_id)

    def run(self) -> None:
        """Start and block until Ctrl+C / SIGTERM, then shut down gracefully."""
        if threading.current_thread() is threading.main_thread() and hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, lambda *_: self._stop.set())
        if not self._started:
            self.start()
        try:
            while not self._stop.wait(0.5):
                pass
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()

    def __enter__(self) -> "BackgroundJobServer":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()

    @property
    def is_running(self) -> bool:
        return self._started and not self._stopped

    # -- live configuration ---------------------------------------------------------

    @property
    def active_workers(self) -> int:
        """Workers currently running and not retiring."""
        with self._scale_lock:
            return sum(1 for w in self._workers if w.is_alive() and not w.retiring.is_set())

    def _start_workers(self, count: int) -> None:
        for _ in range(count):
            worker = Worker(self, next(self._worker_numbers))
            self._workers.append(worker)
            self._threads.append(worker)
            worker.start()

    def set_worker_count(self, count: int) -> None:
        """Scale workers up or down while running. Retiring workers finish their current job first."""
        count = _validate_worker_count(count)
        with self._scale_lock:
            if self._started and not self._stopped:
                self._workers = [w for w in self._workers if w.is_alive() and not w.retiring.is_set()]
                if count > len(self._workers):
                    self._start_workers(count - len(self._workers))
                elif count < len(self._workers):
                    for worker in self._workers[count:]:
                        worker.retiring.set()
                    self._workers = self._workers[:count]
                    self.storage.notify_new_jobs()  # wake idle workers so they notice
            old, self.worker_count = self.worker_count, count
            self._publish_settings()
        if old != count:
            logger.info("Server %s: workers %d -> %d", self.server_id, old, count)

    def set_queues(self, queues: Sequence[str]) -> None:
        """Change the queues (highest priority first). Workers switch on their next fetch."""
        queues = _validate_queues(queues)
        with self._scale_lock:
            old, self.queues = self.queues, queues
            self._publish_settings()
            self.storage.notify_new_jobs()
        if old != queues:
            logger.info("Server %s: queues %s -> %s", self.server_id, ",".join(old), ",".join(queues))

    def _publish_settings(self, applied_request: Optional[dict] = None) -> None:
        if self._started and not self._stopped:
            self.storage.update_server_state(self.server_id, self.worker_count, self.queues, applied_request)

    def _apply_requested_changes(self) -> None:
        record = self.storage.get_server(self.server_id)
        if record is None:
            # Removed by a watchdog (e.g. after a long pause): register again.
            self.storage.announce_server(self.server_id, self.server_name, self.queues, self.worker_count)
            return
        request = record.requested
        if not request:
            return
        with self._scale_lock:
            try:
                if "worker_count" in request:
                    self.set_worker_count(request["worker_count"])
                if "queues" in request:
                    self.set_queues(request["queues"])
            except ValueError:
                logger.exception("Ignoring invalid change request for server %s: %r", self.server_id, request)
            self._publish_settings(applied_request=request)

    # -- background processes -------------------------------------------------------

    def _in_flight_add(self, token) -> None:
        with self._in_flight_lock:
            self._in_flight.add(token)

    def _in_flight_remove(self, token) -> None:
        with self._in_flight_lock:
            self._in_flight.discard(token)

    def _heartbeat(self) -> None:
        with self._scale_lock:
            self._workers = [w for w in self._workers if w.is_alive() or not w.retiring.is_set()]
            self._threads = [t for t in self._threads if t.is_alive() or not isinstance(t, Worker)
                             or not t.retiring.is_set()]
        self.storage.server_heartbeat(self.server_id)
        with self._in_flight_lock:
            tokens = list(self._in_flight)
        self.storage.heartbeat_fetched(tokens)

    def _enqueue_scheduled_jobs(self) -> None:
        lock = self.storage.try_acquire_lock("schedulefire:delayed-scheduler", ttl=60)
        if lock is None:
            return
        with lock:
            machine = StateMachine(self.storage, self.global_filters)
            batch = 1000
            while not self._stop.is_set():
                due = self.storage.get_scheduled_due(time.time(), batch)
                for job_id in due:
                    try:
                        machine.change_state(
                            job_id,
                            EnqueuedState(reason="Triggered by delayed job scheduler"),
                            expected_states=[SCHEDULED],
                        )
                    except Exception:
                        logger.exception("Failed to enqueue scheduled job #%s", job_id)
                if len(due) < batch:
                    break

    def _schedule_recurring_jobs(self) -> None:
        lock = self.storage.try_acquire_lock("schedulefire:recurring-scheduler", ttl=60)
        if lock is None:
            return
        with lock:
            RecurringJobManager(self.storage, global_filters=self.global_filters).schedule_due_jobs()

    def _remove_dead_servers(self) -> None:
        removed = self.storage.remove_timed_out_servers(self.server_timeout)
        if removed:
            logger.info("Removed %d timed-out server(s).", removed)

    def _delete_expired_jobs(self) -> None:
        lock = self.storage.try_acquire_lock("schedulefire:expiration-manager", ttl=300)
        if lock is None:
            return
        with lock:
            total = 0
            while not self._stop.is_set():
                n = self.storage.delete_expired_jobs(time.time())
                total += n
                if n == 0:
                    break
            if total:
                logger.info("Deleted %d expired job(s).", total)

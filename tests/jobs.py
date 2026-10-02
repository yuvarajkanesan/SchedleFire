"""Job functions used by the tests. They must live in an importable module."""

import asyncio
import threading
import time

from schedulefire import AutomaticRetry, DisableConcurrentExecution, get_job_context, queue

calls = []
_lock = threading.Lock()
_attempts = {}
_concurrency = {"now": 0, "max": 0}


def record(name, *args, **kwargs):
    with _lock:
        calls.append((name, args, kwargs))


def reset():
    with _lock:
        calls.clear()
        _attempts.clear()
        _concurrency.update(now=0, max=0)


def add(a, b):
    record("add", a, b)
    return a + b


@AutomaticRetry(attempts=0)
def always_fails(message):
    record("always_fails", message)
    raise ValueError(message)


@AutomaticRetry(attempts=3, delays=[0])
def flaky(key, fail_times):
    with _lock:
        _attempts[key] = _attempts.get(key, 0) + 1
        attempt = _attempts[key]
    if attempt <= fail_times:
        raise RuntimeError(f"attempt {attempt} failed")
    record("flaky", key, attempt)
    return attempt


@AutomaticRetry(attempts=1, delays=[0], on_attempts_exceeded="Delete")
def fails_then_deleted():
    raise RuntimeError("boom")


@queue("critical")
def critical_task(n):
    record("critical", n)


def whoami():
    ctx = get_job_context()
    record("whoami", ctx.job_id, ctx.retry_count)
    return ctx.job_id


async def async_job(x):
    await asyncio.sleep(0.01)
    record("async", x)
    return x * 2


def slow(seconds):
    time.sleep(seconds)
    record("slow", seconds)


@DisableConcurrentExecution(timeout=10)
def exclusive(n):
    with _lock:
        _concurrency["now"] += 1
        _concurrency["max"] = max(_concurrency["max"], _concurrency["now"])
    time.sleep(0.2)
    with _lock:
        _concurrency["now"] -= 1
    record("exclusive", n)


def max_concurrency():
    return _concurrency["max"]


def cooperative_long_job():
    ctx = get_job_context()
    for _ in range(200):
        ctx.throw_if_cancellation_requested()
        time.sleep(0.05)
    record("cooperative_done")


class Greeter:
    instances = 0

    def __init__(self):
        Greeter.instances += 1

    def greet(self, name):
        record("greet", name)
        return f"hello {name}"

    @staticmethod
    def static_greet(name):
        record("static_greet", name)

    @classmethod
    def class_greet(cls, name):
        record("class_greet", cls.__name__, name)

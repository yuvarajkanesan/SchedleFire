"""Job definition: a reference to an importable function plus its arguments.

Like Hangfire stores ``Type + Method + Arguments`` instead of a delegate, schedulefire
stores ``module + qualified name + JSON arguments``. That is why job targets must be
module-level functions, static/class methods, or instance methods referenced through
their class (``EmailService.send``) - the worker imports them by name.
"""

from __future__ import annotations

import asyncio
import fnmatch
import importlib
import importlib.util
import inspect
import os
import re
import sys
import sysconfig
from typing import Any, Callable, Optional, Tuple

from . import _serialization

DEFAULT_QUEUE = "default"

_QUEUE_NAME = re.compile(r"^[a-z0-9_-]+$")


class JobLoadError(Exception):
    """The job's target function could not be imported on the worker."""


class JobSecurityError(JobLoadError):
    """The job's target is not allowed by the job security policy. Never retried."""


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_stdlib_cache: dict = {}


def _is_stdlib(top_level: str) -> bool:
    if top_level in _stdlib_cache:
        return _stdlib_cache[top_level]
    names = getattr(sys, "stdlib_module_names", None)  # Python 3.10+
    if names is not None:
        result = top_level in names
    elif top_level in sys.builtin_module_names:
        result = True
    else:
        try:
            spec = importlib.util.find_spec(top_level)
        except (ImportError, ValueError):
            spec = None
        origin = getattr(spec, "origin", None) if spec else None
        if origin in ("built-in", "frozen"):
            result = True
        elif not origin:
            result = False
        else:
            origin = os.path.normcase(os.path.abspath(origin))
            roots = {os.path.normcase(os.path.abspath(sysconfig.get_paths()[k])) for k in ("stdlib", "platstdlib")}
            result = any(origin.startswith(root + os.sep) for root in roots) and not any(
                part in origin for part in ("site-packages", "dist-packages"))
    _stdlib_cache[top_level] = result
    return result


class JobSecurityPolicy:
    """Decides which functions a worker may run.

    A job is just a module + function name stored in the database, so anyone who can
    write to the job storage could otherwise make workers call *any* importable function
    (``os.system``, ``subprocess.run``, ...). The policy is checked when a job is created
    and - authoritatively - on the worker right before it runs.

    :param allowed_modules: if given, only these modules may contain job targets. An entry
        ``"myapp"`` allows ``myapp`` and every submodule; glob patterns such as
        ``"myapp.*.tasks"`` work too. Recommended for production.
    :param allow_stdlib: allow targets in the Python standard library (blocked by default:
        it contains ``os``, ``subprocess``, ``shutil``, ``builtins``, ...).
    :param blocked_modules: extra modules to block (same matching as ``allowed_modules``).
    """

    ALWAYS_BLOCKED = ("schedulefire",)

    def __init__(self, allowed_modules=None, *, allow_stdlib: bool = False, blocked_modules=()):
        self.allowed_modules = None if allowed_modules is None else list(allowed_modules)
        self.allow_stdlib = allow_stdlib
        self.blocked_modules = list(blocked_modules)

    @staticmethod
    def _matches(module: str, patterns) -> bool:
        for pattern in patterns:
            if any(ch in pattern for ch in "*?["):
                if fnmatch.fnmatchcase(module, pattern):
                    return True
            elif module == pattern or module.startswith(pattern + "."):
                return True
        return False

    def check(self, module: str, qualname: str) -> None:
        """Raise JobSecurityError unless ``module.qualname`` may be run as a job."""
        target = f"{module}.{qualname}"
        mod_parts, name_parts = module.split("."), qualname.split(".")
        if not all(_IDENTIFIER.match(p) for p in mod_parts + name_parts):
            raise JobSecurityError(f"Refusing job target {target!r}: not a plain dotted name.")
        if any(p.startswith("__") for p in name_parts):
            raise JobSecurityError(f"Refusing job target {target!r}: dunder attributes are not allowed.")
        if self._matches(module, self.ALWAYS_BLOCKED) or self._matches(module, self.blocked_modules):
            raise JobSecurityError(f"Refusing job target {target!r}: module {module!r} is blocked.")
        if self.allowed_modules is not None:
            if not self._matches(module, self.allowed_modules):
                raise JobSecurityError(
                    f"Refusing job target {target!r}: module {module!r} is not in allowed_job_modules.")
        elif not self.allow_stdlib and module != "__main__" and _is_stdlib(mod_parts[0]):
            raise JobSecurityError(
                f"Refusing job target {target!r}: standard-library functions can't be jobs "
                "(set allow_stdlib_jobs to permit them).")

    def __repr__(self) -> str:
        return (f"JobSecurityPolicy(allowed_modules={self.allowed_modules!r}, allow_stdlib={self.allow_stdlib}, "
                f"blocked_modules={self.blocked_modules!r})")


_policy = JobSecurityPolicy()


def set_job_security_policy(policy: JobSecurityPolicy) -> None:
    """Set the process-wide job security policy (``schedulefire.configure(job_security=...)``)."""
    global _policy
    _policy = policy


def get_job_security_policy() -> JobSecurityPolicy:
    return _policy


def validate_queue_name(name: str) -> str:
    if not isinstance(name, str) or not _QUEUE_NAME.match(name):
        raise ValueError(
            f"Invalid queue name {name!r}: use only lowercase letters, digits, '_' and '-'."
        )
    return name


def queue(name: str) -> Callable:
    """Route a job function to a queue - the equivalent of Hangfire's ``[Queue("name")]``.

    ::

        @queue("critical")
        def charge_card(order_id): ...
    """
    validate_queue_name(name)

    def decorator(target):
        holder = target.__func__ if isinstance(target, (staticmethod, classmethod)) else target
        holder.__schedulefire_queue__ = name
        return target

    return decorator


class JobActivator:
    """Creates instances for jobs that target instance methods (Hangfire's ``JobActivator``).

    Subclass it and pass it to ``schedulefire.configure(activator=...)`` or to the server to
    plug in a dependency-injection container.
    """

    def activate(self, cls: type) -> Any:
        return cls()


def _main_module_name() -> str:
    # When a script runs with `python -m package.module`, recover the importable name.
    main = sys.modules.get("__main__")
    spec = getattr(main, "__spec__", None)
    if spec is not None and spec.name and spec.name != "__main__":
        return spec.name
    return "__main__"


def _target_path(func: Any) -> Tuple[str, str]:
    if isinstance(func, (staticmethod, classmethod)):
        func = func.__func__
    if inspect.ismethod(func):
        owner = func.__self__
        if not inspect.isclass(owner):
            raise TypeError(
                "A bound instance method can't be a job target because the instance can't be "
                "serialized. Pass the method through its class instead (e.g. `EmailService.send`); "
                "the worker creates the instance with the JobActivator."
            )
        # classmethod: keep the class it was accessed through, not where it was defined.
        return owner.__module__, f"{owner.__qualname__}.{func.__name__}"
    if inspect.isclass(func):
        raise TypeError(f"Job target must be a function or method, not a class: {func!r}.")
    if not callable(func) or not hasattr(func, "__qualname__") or not hasattr(func, "__module__"):
        raise TypeError(f"Job target must be a function or method, got {func!r}.")
    qualname = func.__qualname__
    if "<lambda>" in qualname:
        raise TypeError("Lambdas can't be job targets: define a module-level function instead.")
    if "<locals>" in qualname:
        raise TypeError(
            f"{qualname} is a nested function and can't be imported by the worker: "
            "define it at module level."
        )
    module = func.__module__
    if module == "__main__":
        module = _main_module_name()
    return module, qualname


class Job:
    """A serializable call: ``Job(send_email, args=("a@b.com",), kwargs={"subject": "Hi"})``.

    Most code never builds a ``Job`` directly - ``BackgroundJob.enqueue(func, *args, **kwargs)``
    does it. Build one when you need to pass options such as ``queue``.
    """

    def __init__(
        self,
        func: Callable,
        args: tuple = (),
        kwargs: Optional[dict] = None,
        *,
        queue: Optional[str] = None,
    ):
        self.module, self.qualname = _target_path(func)
        _policy.check(self.module, self.qualname)
        self.args = tuple(args)
        self.kwargs = dict(kwargs or {})
        holder = func.__func__ if isinstance(func, (staticmethod, classmethod)) else func
        self.queue = validate_queue_name(
            queue or getattr(holder, "__schedulefire_queue__", None) or DEFAULT_QUEUE
        )
        # Fail at enqueue time, not on the worker.
        _serialization.dumps([self.args, self.kwargs])

    @classmethod
    def from_invocation(cls, invocation: dict, queue: str = DEFAULT_QUEUE) -> "Job":
        job = cls.__new__(cls)
        job.module = invocation["module"]
        job.qualname = invocation["qualname"]
        job.args = tuple(invocation.get("args", ()))
        job.kwargs = dict(invocation.get("kwargs", {}))
        job.queue = queue
        return job

    def to_invocation(self) -> dict:
        return {
            "module": self.module,
            "qualname": self.qualname,
            "args": list(self.args),
            "kwargs": self.kwargs,
        }

    @property
    def method_name(self) -> str:
        return f"{self.module}.{self.qualname}"

    def display_name(self, max_len: int = 120) -> str:
        parts = [repr(a) for a in self.args] + [f"{k}={v!r}" for k, v in self.kwargs.items()]
        text = f"{self.qualname}({', '.join(parts)})"
        return text if len(text) <= max_len else text[: max_len - 3] + "..."

    def load(self) -> Tuple[Callable, Optional[type]]:
        """Import the target. Returns ``(function, owner_class_or_None)`` without instantiating.

        Enforces the job security policy before importing anything.
        """
        _policy.check(self.module, self.qualname)
        try:
            obj: Any = importlib.import_module(self.module)
        except ImportError as e:
            raise JobLoadError(f"Could not import module '{self.module}': {e}") from e
        owner: Any = None
        for part in self.qualname.split("."):
            owner = obj
            try:
                obj = getattr(obj, part)
            except AttributeError as e:
                raise JobLoadError(f"'{self.module}' has no attribute '{self.qualname}'") from e
        if inspect.isclass(obj) or not (inspect.isroutine(obj) or inspect.ismethod(obj)):
            raise JobSecurityError(f"Refusing job target '{self.method_name}': it is not a function or method.")
        return obj, owner if inspect.isclass(owner) else None

    def create_callable(self, activator: Optional[JobActivator] = None) -> Callable:
        """Import the target and, for instance methods, create the instance via the activator."""
        target, owner = self.load()
        if owner is not None and inspect.isfunction(target):
            raw = inspect.getattr_static(owner, self.qualname.rsplit(".", 1)[-1])
            if not isinstance(raw, (staticmethod, classmethod)):
                instance = (activator or JobActivator()).activate(owner)
                return getattr(instance, target.__name__)
        return target

    def invoke(self, activator: Optional[JobActivator] = None) -> Any:
        """Run the job in the current thread. Coroutine functions are run with ``asyncio.run``."""
        result = self.create_callable(activator)(*self.args, **self.kwargs)
        if inspect.isawaitable(result):
            result = asyncio.run(_await(result))
        return result

    def __repr__(self) -> str:
        return f"<Job {self.module}.{self.display_name()} queue={self.queue!r}>"


async def _await(awaitable: Any) -> Any:
    return await awaitable

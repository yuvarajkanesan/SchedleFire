"""Configuration file support.

One file (``schedulefire.toml`` or ``schedulefire.json``) holds every setting: connection
details for any number of databases (pick the active one with ``[storage] use = "..."``),
workers, queues, retries, job expiration, the dashboard, logging and recurring jobs.

Where settings come from (first wins):

1. arguments passed in code / on the command line
2. environment variables ``SCHEDULEFIRE_DB`` (a storage URL) and ``SCHEDULEFIRE_STORAGE``
   (which ``[storage.*]`` profile to use)
3. the config file: ``SCHEDULEFIRE_CONFIG``, else ``schedulefire.toml`` / ``schedulefire.json``
   in the working directory
4. built-in defaults

Any string value may reference environment variables: ``${DB_PASSWORD}`` or
``${DB_HOST:-localhost}`` (with a default). Keep secrets in the environment, not the file.
"""

from __future__ import annotations

import difflib
import importlib
import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import quote, urlencode

logger = logging.getLogger("schedulefire")

CONFIG_FILENAMES = ("schedulefire.toml", "schedulefire.json")

# canonical type -> accepted spellings
_TYPE_ALIASES = {
    "sqlite": "sqlite", "sqlite3": "sqlite",
    "postgres": "postgres", "postgresql": "postgres", "pg": "postgres",
    "mysql": "mysql", "mariadb": "mysql",
    "sqlserver": "sqlserver", "mssql": "sqlserver", "azuresql": "sqlserver",
    "redis": "redis",
    "memory": "memory",
}
_COMMON_STORAGE_KEYS = {"type", "job_expiration", "invisibility_timeout"}
_STORAGE_KEYS = {
    "sqlite": {"path", "busy_timeout"},
    "postgres": {"url", "host", "port", "user", "password", "database", "schema", "create_database",
                 "prepare_schema", "use_notify", "options"},
    "mysql": {"url", "host", "port", "user", "password", "database", "table_prefix", "create_database",
              "prepare_schema", "options"},
    "sqlserver": {"connection", "url", "host", "port", "user", "password", "database", "driver", "encrypt",
                  "trust_server_certificate", "schema", "create_database", "prepare_schema", "options"},
    "redis": {"url", "host", "port", "password", "db", "ssl", "prefix", "use_notify"},
    "memory": set(),
}
_STORAGE_TYPES_OF = {
    **{k: str for k in ("url", "host", "user", "password", "database", "schema", "path", "table_prefix", "prefix",
                        "connection", "driver")},
    **{k: int for k in ("port", "db")},
    **{k: bool for k in ("create_database", "prepare_schema", "use_notify", "ssl", "encrypt",
                         "trust_server_certificate")},
    "options": dict,
    "busy_timeout": float,
}
_SECTIONS = {"storage", "jobs", "retry", "server", "dashboard", "logging", "recurring", "security"}
_SECRET_KEYS = re.compile(r"(?i)password|pwd|secret|token")
_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
_DURATION = re.compile(r"(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)")
_UNITS = {"ms": 0.001, "s": 1, "m": 60, "h": 3600, "d": 86400}


class ConfigError(ValueError):
    """The configuration file is invalid. The message names the file and the key."""


# ---------------------------------------------------------------------- values


def parse_duration(value: Any, where: str) -> float:
    """Seconds from a number or a string like "500ms", "15s", "5m", "1h30m", "1d"."""
    if isinstance(value, bool):
        raise ConfigError(f"{where}: expected a duration, got {value!r}")
    if isinstance(value, (int, float)):
        if value < 0:
            raise ConfigError(f"{where}: must not be negative")
        return float(value)
    if isinstance(value, str):
        text = value.strip().lower().replace(" ", "")
        if re.fullmatch(r"\d+(\.\d+)?", text):
            return float(text)
        parts = _DURATION.findall(text)
        if parts and "".join(n + u for n, u in parts) == text:
            return sum(float(n) * _UNITS[u] for n, u in parts)
    raise ConfigError(f'{where}: invalid duration {value!r} (use seconds or e.g. "500ms", "15s", "5m", "1h", "1d")')


def _shown(value: Any, where: str) -> str:
    """Repr for error messages - never echo secrets."""
    return "****" if _SECRET_KEYS.search(where) else repr(value)


def _expect(value: Any, kind: type, where: str) -> Any:
    ok = isinstance(value, kind) and not (kind in (int, float) and isinstance(value, bool))
    if kind is float and isinstance(value, int) and not isinstance(value, bool):
        ok = True
    if not ok:
        names = {str: "a string", int: "an integer", float: "a number", bool: "true or false",
                 list: "a list", dict: "a table"}
        raise ConfigError(f"{where}: expected {names.get(kind, kind.__name__)}, got {_shown(value, where)}")
    return value


def _str_list(value: Any, where: str) -> List[str]:
    if isinstance(value, str):
        value = [v.strip() for v in value.split(",") if v.strip()]
    _expect(value, list, where)
    for item in value:
        _expect(item, str, where)
    return list(value)


def _check_keys(table: Mapping[str, Any], allowed: set, where: str) -> None:
    for key in table:
        if key not in allowed:
            hint = difflib.get_close_matches(key, sorted(allowed), n=1)
            suggestion = f' - did you mean "{hint[0]}"?' if hint else f" (allowed: {', '.join(sorted(allowed))})"
            raise ConfigError(f'{where}: unknown setting "{key}"{suggestion}')


def _interpolate(value: Any, environ: Mapping[str, str], where: str) -> Any:
    if isinstance(value, str):
        def replace(m):
            name, default = m.group(1), m.group(2)
            if name in environ:
                return environ[name]
            if default is not None:
                return default
            raise ConfigError(f"{where}: environment variable {name} is not set "
                              f"(set it, or give a default: ${{{name}:-value}})")
        return _ENV_REF.sub(replace, value)
    if isinstance(value, dict):
        return {k: _interpolate(v, environ, f"{where}.{k}") for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate(v, environ, f"{where}[{i}]") for i, v in enumerate(value)]
    return value


def mask_secrets(text: str) -> str:
    """Hide passwords in URLs and ODBC strings."""
    text = re.sub(r"(://[^:/@]*:)[^@]*@", r"\1****@", text)
    return re.sub(r"(?i)((?:pwd|password)\s*=\s*)(\{[^}]*\}|[^;]*)", r"\1****", text)


# ---------------------------------------------------------------------- settings


@dataclass
class StorageProfile:
    name: str
    type: str
    options: Dict[str, Any]

    def create(self, jobs: "JobsSettings"):
        from .storage import (MemoryStorage, MySQLStorage, PostgreSQLStorage, RedisStorage, SQLiteStorage,
                              SQLServerStorage)

        o = dict(self.options)
        common = {
            "job_expiration": o.pop("job_expiration", jobs.expiration),
            "invisibility_timeout": o.pop("invisibility_timeout", jobs.invisibility_timeout),
        }
        if self.type == "memory":
            return MemoryStorage(**common)
        if self.type == "sqlite":
            return SQLiteStorage(o.pop("path", "schedulefire.db"), **o, **common)
        if self.type == "postgres":
            return PostgreSQLStorage(_postgres_dsn(o), **o, **common)
        if self.type == "mysql":
            url = _mysql_url(o)
            extra = o.pop("options", {})
            return MySQLStorage(url, **o, **extra, **common)
        if self.type == "sqlserver":
            return SQLServerStorage(_sqlserver_connection(o), **o, **common)
        if self.type == "redis":
            return RedisStorage(_redis_url(o), **o, **{k: v for k, v in common.items()})
        raise ConfigError(f"Unknown storage type {self.type!r}")

    def describe(self) -> str:
        parts = []
        for key, value in self.options.items():
            if _SECRET_KEYS.search(key):
                value = "****"
            elif isinstance(value, str):
                value = mask_secrets(value)
            parts.append(f"{key}={value}")
        return f"{self.name} (type {self.type}){': ' + ', '.join(parts) if parts else ''}"


def _pop_fields(o: Dict[str, Any], *names: str) -> Dict[str, Any]:
    return {n: o.pop(n) for n in names if n in o}


def _url_auth(user: Optional[str], password: Optional[str]) -> str:
    if not user:
        return ""
    return quote(user, safe="") + (":" + quote(password, safe="") if password else "") + "@"


def _postgres_dsn(o: Dict[str, Any]) -> str:
    f = _pop_fields(o, "url", "host", "port", "user", "password", "database")
    extra = o.pop("options", {})
    if "url" in f:
        url = f["url"]
    else:
        url = (f"postgresql://{_url_auth(f.get('user'), f.get('password'))}{f.get('host', 'localhost')}"
               f":{f.get('port', 5432)}/{quote(f.get('database', 'postgres'), safe='')}")
    if extra:
        url += ("&" if "?" in url else "?") + urlencode(extra)
    return url


def _mysql_url(o: Dict[str, Any]) -> str:
    f = _pop_fields(o, "url", "host", "port", "user", "password", "database")
    if "url" in f:
        return f["url"]
    if "database" not in f:
        raise ConfigError("MySQL storage needs `database` (or `url`)")
    return (f"mysql://{_url_auth(f.get('user', 'root'), f.get('password'))}{f.get('host', 'localhost')}"
            f":{f.get('port', 3306)}/{quote(f['database'], safe='')}")


def _sqlserver_connection(o: Dict[str, Any]) -> str:
    f = _pop_fields(o, "connection", "url", "host", "port", "user", "password", "database", "driver",
                    "encrypt", "trust_server_certificate")
    extra = {k: str(v) for k, v in o.pop("options", {}).items()}
    if "connection" in f or "url" in f:
        return f.get("connection") or f["url"]
    if "database" not in f:
        raise ConfigError("SQL Server storage needs `database` (or `connection` / `url`)")
    host = f.get("host", "localhost")
    params: Dict[str, str] = {}
    if "driver" in f:
        params["driver"] = f["driver"]
    if "encrypt" in f:
        params["Encrypt"] = "yes" if f["encrypt"] else "no"
    if "trust_server_certificate" in f:
        params["TrustServerCertificate"] = "yes" if f["trust_server_certificate"] else "no"
    params.update(extra)
    port = f":{f['port']}" if "port" in f else ""
    query = ("?" + urlencode(params)) if params else ""
    return f"mssql://{_url_auth(f.get('user'), f.get('password'))}{quote(host, safe='')}{port}/" \
           f"{quote(f['database'], safe='')}{query}"


def _redis_url(o: Dict[str, Any]) -> str:
    f = _pop_fields(o, "url", "host", "port", "password", "db", "ssl")
    if "url" in f:
        return f["url"]
    scheme = "rediss" if f.get("ssl") else "redis"
    auth = ":" + quote(f["password"], safe="") + "@" if f.get("password") else ""
    return f"{scheme}://{auth}{f.get('host', 'localhost')}:{f.get('port', 6379)}/{f.get('db', 0)}"


@dataclass
class JobsSettings:
    expiration: float = 24 * 3600
    invisibility_timeout: float = 30 * 60


@dataclass
class RetrySettings:
    attempts: int = 10
    delays: Optional[List[float]] = None
    on_attempts_exceeded: str = "fail"
    only_on: List[str] = field(default_factory=list)
    except_on: List[str] = field(default_factory=list)

    def create_filter(self):
        from .filters import AttemptsExceededAction, AutomaticRetry

        def classes(names: Sequence[str]):
            return tuple(_import_object(n, "exception class") for n in names) or None

        return AutomaticRetry(
            self.attempts,
            delays=self.delays,
            on_attempts_exceeded=AttemptsExceededAction(self.on_attempts_exceeded.capitalize()),
            only_on=classes(self.only_on),
            except_on=classes(self.except_on),
        )


@dataclass
class ServerSettings:
    name: Optional[str] = None
    workers: Optional[int] = None
    queues: List[str] = field(default_factory=lambda: ["default"])
    imports: List[str] = field(default_factory=list)
    queue_poll_interval: float = 1.0
    schedule_polling_interval: float = 15.0
    heartbeat_interval: float = 30.0
    server_timeout: float = 300.0
    server_check_interval: float = 60.0
    expiration_check_interval: float = 1800.0
    shutdown_timeout: float = 15.0
    control_poll_interval: float = 5.0

    def server_kwargs(self) -> Dict[str, Any]:
        return {
            "queues": self.queues, "worker_count": self.workers, "server_name": self.name,
            "queue_poll_interval": self.queue_poll_interval,
            "schedule_polling_interval": self.schedule_polling_interval,
            "heartbeat_interval": self.heartbeat_interval, "server_timeout": self.server_timeout,
            "server_check_interval": self.server_check_interval,
            "expiration_check_interval": self.expiration_check_interval,
            "shutdown_timeout": self.shutdown_timeout, "control_poll_interval": self.control_poll_interval,
        }


_OAUTH_KEYS = {"provider", "client_id", "client_secret", "issuer", "tenant", "authorize_url", "token_url",
               "userinfo_url", "scopes", "redirect_uri", "allowed_emails", "allowed_domains", "allowed_users",
               "allow_any_user", "secret_key", "session_lifetime", "idle_timeout"}
_OAUTH_DURATION_KEYS = {"session_lifetime", "idle_timeout"}
_AUTH_MODES = ("none", "local", "basic", "oauth")


@dataclass
class DashboardSettings:
    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 8080
    #: URL path the standalone dashboard is served under ("/" for the site root).
    path: str = "/schedule-fire"
    title: str = "scheduleFire"
    read_only: bool = False
    page_size: int = 20
    #: "none" | "local" | "basic" | "oauth"; None = inferred from the settings present.
    auth: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    users: Dict[str, str] = field(default_factory=dict)
    oauth: Dict[str, Any] = field(default_factory=dict)
    routes: Dict[str, Any] = field(default_factory=dict)
    #: Seconds of inactivity before a signed-in browser session (basic/oauth) is signed out.
    #: None/0 disables it. [dashboard.oauth].idle_timeout overrides this for oauth specifically.
    idle_timeout: Optional[float] = 300.0
    #: Signs basic-auth session cookies. Auto-generated (with a warning) if not set.
    secret_key: Optional[str] = None

    @property
    def auth_mode(self) -> str:
        if self.auth:
            return self.auth
        if self.oauth:
            return "oauth"
        if self.username or self.users:
            return "basic"
        return "none"

    @property
    def effective_idle_timeout(self) -> Optional[float]:
        """The idle timeout that actually applies: [dashboard.oauth].idle_timeout overrides
        the top-level [dashboard] one when set; used by both create_auth() and describe()."""
        if self.auth_mode == "oauth" and "idle_timeout" in self.oauth:
            return self.oauth["idle_timeout"] or None
        return self.idle_timeout

    def create_auth(self):
        from .dashboard.auth import BasicAuth, OAuth

        mode = self.auth_mode
        if mode == "basic":
            users = dict(self.users)
            if self.username:
                users[self.username] = self.password or ""
            return BasicAuth(users, idle_timeout=self.idle_timeout, secret_key=self.secret_key)
        if mode == "oauth":
            kwargs = dict(self.oauth)
            kwargs["idle_timeout"] = self.effective_idle_timeout
            return OAuth(**kwargs)
        return mode

    def app_kwargs(self) -> Dict[str, Any]:
        return {"title": self.title, "read_only": self.read_only, "page_size": self.page_size,
                "auth": self.create_auth(), "routes": self.routes or None}


@dataclass
class RecurringSettings:
    id: str
    job: str
    cron: str
    time_zone: Optional[str] = None
    queue: Optional[str] = None
    args: List[Any] = field(default_factory=list)
    kwargs: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SecuritySettings:
    allowed_job_modules: Optional[List[str]] = None
    allow_stdlib_jobs: bool = False
    blocked_job_modules: List[str] = field(default_factory=list)

    def create_policy(self):
        from .job import JobSecurityPolicy

        return JobSecurityPolicy(self.allowed_job_modules, allow_stdlib=self.allow_stdlib_jobs,
                                 blocked_modules=self.blocked_job_modules)


@dataclass
class Settings:
    path: Optional[str] = None
    security: SecuritySettings = field(default_factory=SecuritySettings)
    storage: StorageProfile = field(default_factory=lambda: StorageProfile("sqlite", "sqlite", {}))
    storages: Dict[str, StorageProfile] = field(default_factory=dict)
    jobs: JobsSettings = field(default_factory=JobsSettings)
    retry: RetrySettings = field(default_factory=RetrySettings)
    server: ServerSettings = field(default_factory=ServerSettings)
    dashboard: DashboardSettings = field(default_factory=DashboardSettings)
    log_level: str = "INFO"
    log_format: str = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"
    recurring: List[RecurringSettings] = field(default_factory=list)

    # -- building things ------------------------------------------------------------

    def create_storage(self, profile: Optional[str] = None):
        """Create the active storage (or the named ``[storage.<profile>]``)."""
        chosen = self.storage if profile is None else self.storages.get(profile)
        if chosen is None:
            raise ConfigError(f"No storage named {profile!r}; defined: {', '.join(self.storages) or 'none'}")
        return chosen.create(self.jobs)

    def apply(self, *, storage=None, configure_logging: bool = False, register_recurring: bool = False):
        """Make these settings the process defaults. Returns the storage.

        Sets the global storage and retry policy; optionally configures logging and
        creates/updates the ``[[recurring]]`` jobs.
        """
        from . import config
        from .filters import AutomaticRetry, GlobalJobFilters

        if configure_logging:
            logging.basicConfig(level=self.log_level.upper(), format=self.log_format)
        config.configure(job_security=self.security.create_policy())
        storage = storage or self.create_storage()
        config.configure(storage=storage)
        GlobalJobFilters.remove(AutomaticRetry)
        GlobalJobFilters.add(self.retry.create_filter())
        for module in self.server.imports:
            importlib.import_module(module)
        if register_recurring:
            self.register_recurring_jobs(storage)
        return storage

    def register_recurring_jobs(self, storage) -> None:
        from .client import RecurringJobManager
        from .job import Job

        manager = RecurringJobManager(storage)
        for r in self.recurring:
            func = _import_object(r.job, "job function")
            manager.add_or_update(r.id, Job(func, args=tuple(r.args), kwargs=r.kwargs, queue=r.queue),
                                  r.cron, time_zone=r.time_zone)
            logger.info("Recurring job %r registered from config (%s)", r.id, r.cron)

    def create_server(self, storage=None, **overrides):
        from .server import BackgroundJobServer

        kwargs = self.server.server_kwargs()
        kwargs.update({k: v for k, v in overrides.items() if v is not None})
        return BackgroundJobServer(storage, **kwargs)

    def describe(self) -> str:
        from .dashboard.auth import format_duration as _format_duration

        lines = [f"config file : {self.path or '(none - using defaults)'}",
                 f"storage     : {self.storage.describe()}"]
        others = [p.name for p in self.storages.values() if p.name != self.storage.name]
        if others:
            lines.append(f"also defined: {', '.join(others)}")
        lines += [
            f"jobs        : expire after {self.jobs.expiration:g}s, invisibility timeout {self.jobs.invisibility_timeout:g}s",
            f"retry       : {self.retry.attempts} attempts"
            + (f", delays {self.retry.delays}" if self.retry.delays else "")
            + f", then {self.retry.on_attempts_exceeded}",
            f"server      : {self.server.workers or 'auto'} workers, queues {', '.join(self.server.queues)}"
            + (f", name {self.server.name}" if self.server.name else ""),
            f"dashboard   : {'on' if self.dashboard.enabled else 'off'} at "
            f"{self.dashboard.host}:{self.dashboard.port}{'' if self.dashboard.path == '/' else self.dashboard.path}"
            f", auth {self.dashboard.auth_mode}"
            + (f", idle timeout {_format_duration(self.dashboard.effective_idle_timeout)}"
               if self.dashboard.effective_idle_timeout and self.dashboard.auth_mode in ("basic", "oauth") else "")
            + (", read-only" if self.dashboard.read_only else "")
            + (", custom routes: " + ", ".join("%s=%s" % (k, v or "off") for k, v in self.dashboard.routes.items())
               if self.dashboard.routes else ""),
            f"logging     : {self.log_level}",
            "security    : jobs limited to "
            + (", ".join(self.security.allowed_job_modules) if self.security.allowed_job_modules is not None
               else "any non-standard-library module")
            + (" (+ standard library)" if self.security.allow_stdlib_jobs else ""),
            f"recurring   : {', '.join(f'{r.id} ({r.cron})' for r in self.recurring) or 'none'}",
        ]
        return "\n".join(lines)


def _validate_oauth(options: Dict[str, Any]) -> None:
    """Check [dashboard.oauth] without network access (the same rules OAuth() enforces)."""
    from .dashboard.auth import PROVIDERS

    provider = str(options.get("provider", "oidc")).lower()
    if provider not in PROVIDERS:
        raise ValueError(f"unknown provider {provider!r}; use one of {', '.join(PROVIDERS)}")
    if not options.get("client_id"):
        raise ValueError("client_id is required")
    if provider == "oidc" and not options.get("issuer") and not all(
            options.get(k) for k in ("authorize_url", "token_url", "userinfo_url")):
        raise ValueError("provider 'oidc' needs `issuer` (or authorize_url, token_url and userinfo_url)")
    if not (options.get("allowed_emails") or options.get("allowed_domains") or options.get("allowed_users")
            or options.get("allow_any_user")):
        raise ValueError("set allowed_emails, allowed_domains or allowed_users (or allow_any_user = true to admit "
                         "every account the provider accepts)")
    if options.get("secret_key") is not None and len(options["secret_key"]) < 32:
        raise ValueError("secret_key must be at least 32 characters")


def _import_object(path: str, what: str):
    """Import "package.module:Name.attr" or "package.module.Name"."""
    if ":" in path:
        module_name, _, attr_path = path.partition(":")
    else:
        module_name, _, attr_path = path.rpartition(".")
    try:
        obj = importlib.import_module(module_name)
        for part in attr_path.split("."):
            obj = getattr(obj, part)
        return obj
    except (ImportError, AttributeError, ValueError) as e:
        raise ConfigError(f"Cannot import {what} {path!r}: {e}") from None


# ---------------------------------------------------------------------- loading


def find_config_file(environ: Optional[Mapping[str, str]] = None, directory: Optional[str] = None) -> Optional[str]:
    environ = os.environ if environ is None else environ
    explicit = environ.get("SCHEDULEFIRE_CONFIG")
    if explicit:
        if not os.path.exists(explicit):
            raise ConfigError(f"SCHEDULEFIRE_CONFIG points to {explicit!r}, which does not exist")
        return explicit
    for name in CONFIG_FILENAMES:
        candidate = os.path.join(directory or os.getcwd(), name)
        if os.path.exists(candidate):
            return candidate
    return None


def _read_file(path: str) -> Dict[str, Any]:
    with open(path, "rb") as f:
        data = f.read()
    try:
        if path.lower().endswith(".json"):
            return json.loads(data.decode("utf-8"))
        try:
            import tomllib  # Python 3.11+
        except ImportError:
            try:
                import tomli as tomllib  # type: ignore[no-redef]
            except ImportError:
                raise ConfigError(f"{path}: reading TOML on Python < 3.11 needs the tomli package, which "
                                  "`pip install schedulefire` installs; or use schedulefire.json") from None
        return tomllib.loads(data.decode("utf-8"))
    except ConfigError:
        raise
    except Exception as e:
        raise ConfigError(f"{path}: cannot parse file: {e}") from None


def _literal_secrets(value: Any, where: str = "") -> List[str]:
    """Keys that hold a password written directly in the file (not ``${ENV_VAR}``)."""
    found: List[str] = []
    if isinstance(value, dict):
        for k, v in value.items():
            path = f"{where}.{k}" if where else str(k)
            is_user_password = where.endswith("dashboard.users") and not v.startswith("pbkdf2_sha256$") \
                if isinstance(v, str) else False
            if isinstance(v, str) and v and (_SECRET_KEYS.search(str(k)) or is_user_password) and "${" not in v:
                found.append(path)
            elif isinstance(v, str) and re.search(r"://[^:/@]+:[^@${]+@", v):
                found.append(path)  # password inside a URL
            else:
                found += _literal_secrets(v, path)
    elif isinstance(value, list):
        for i, v in enumerate(value):
            found += _literal_secrets(v, f"{where}[{i}]")
    return found


def _warn_about_exposed_secrets(path: str, raw: Dict[str, Any]) -> None:
    secrets = _literal_secrets(raw)
    if not secrets:
        return
    readable_by_others = os.name == "posix" and bool(os.stat(path).st_mode & 0o077)
    logger.warning(
        "%s contains passwords written in the file (%s). Prefer ${ENV_VAR} references%s.",
        path, ", ".join(secrets),
        "; the file is also readable by other users - run: chmod 600 " + path if readable_by_others else "",
    )


def load_config(path: Optional[str] = None, *, environ: Optional[Mapping[str, str]] = None) -> Settings:
    """Load and validate a config file (found automatically when ``path`` is None).

    With no file at all, returns the defaults (SQLite at ./schedulefire.db).
    """
    environ = os.environ if environ is None else environ
    path = path or find_config_file(environ)
    raw: Dict[str, Any] = {}
    if path:
        raw = _read_file(path)
        _expect(raw, dict, path)
        _warn_about_exposed_secrets(path, raw)
        raw = _interpolate(raw, environ, os.path.basename(path))
    return parse_settings(raw, path=path, environ=environ)


def parse_settings(raw: Mapping[str, Any], *, path: Optional[str] = None,
                   environ: Optional[Mapping[str, str]] = None) -> Settings:
    environ = os.environ if environ is None else environ
    src = os.path.basename(path) if path else "config"
    _check_keys(raw, _SECTIONS, src)
    settings = Settings(path=path)

    # [jobs]
    jobs = _expect(raw.get("jobs", {}), dict, f"{src} [jobs]")
    _check_keys(jobs, {"expiration", "invisibility_timeout"}, f"{src} [jobs]")
    if "expiration" in jobs:
        settings.jobs.expiration = parse_duration(jobs["expiration"], f"{src} jobs.expiration")
    if "invisibility_timeout" in jobs:
        settings.jobs.invisibility_timeout = parse_duration(jobs["invisibility_timeout"], f"{src} jobs.invisibility_timeout")

    # [storage]
    settings.storages, settings.storage = _parse_storage(raw.get("storage", {}), src, environ)

    # [retry]
    retry = _expect(raw.get("retry", {}), dict, f"{src} [retry]")
    _check_keys(retry, {"attempts", "delays", "on_attempts_exceeded", "only_on", "except_on"}, f"{src} [retry]")
    if "attempts" in retry:
        settings.retry.attempts = _expect(retry["attempts"], int, f"{src} retry.attempts")
        if settings.retry.attempts < 0:
            raise ConfigError(f"{src} retry.attempts: must be >= 0")
    if "delays" in retry:
        delays = _expect(retry["delays"], list, f"{src} retry.delays")
        settings.retry.delays = [parse_duration(d, f"{src} retry.delays[{i}]") for i, d in enumerate(delays)] or None
    if "on_attempts_exceeded" in retry:
        action = _expect(retry["on_attempts_exceeded"], str, f"{src} retry.on_attempts_exceeded").lower()
        if action not in ("fail", "delete"):
            raise ConfigError(f'{src} retry.on_attempts_exceeded: must be "fail" or "delete"')
        settings.retry.on_attempts_exceeded = action
    for key in ("only_on", "except_on"):
        if key in retry:
            setattr(settings.retry, key, _str_list(retry[key], f"{src} retry.{key}"))

    # [server]
    server = _expect(raw.get("server", {}), dict, f"{src} [server]")
    durations = {"queue_poll_interval", "schedule_polling_interval", "heartbeat_interval", "server_timeout",
                 "server_check_interval", "expiration_check_interval", "shutdown_timeout", "control_poll_interval"}
    _check_keys(server, {"name", "workers", "queues", "imports"} | durations, f"{src} [server]")
    s = settings.server
    if "name" in server:
        s.name = _expect(server["name"], str, f"{src} server.name")
    if "workers" in server:
        s.workers = _expect(server["workers"], int, f"{src} server.workers")
        if not 1 <= s.workers <= 1000:
            raise ConfigError(f"{src} server.workers: must be between 1 and 1000")
    if "queues" in server:
        from .job import validate_queue_name

        s.queues = _str_list(server["queues"], f"{src} server.queues")
        try:
            for q in s.queues:
                validate_queue_name(q)
        except ValueError as e:
            raise ConfigError(f"{src} server.queues: {e}") from None
        if not s.queues:
            raise ConfigError(f"{src} server.queues: at least one queue is required")
    if "imports" in server:
        s.imports = _str_list(server["imports"], f"{src} server.imports")
    for key in durations & set(server):
        setattr(s, key, parse_duration(server[key], f"{src} server.{key}"))

    # [dashboard]
    dash = _expect(raw.get("dashboard", {}), dict, f"{src} [dashboard]")
    _check_keys(dash, {"enabled", "host", "port", "path", "title", "read_only", "username", "password", "page_size",
                       "auth", "users", "oauth", "routes", "idle_timeout", "secret_key"}, f"{src} [dashboard]")
    d = settings.dashboard
    for key, kind in (("enabled", bool), ("host", str), ("port", int), ("path", str), ("title", str),
                      ("read_only", bool), ("username", str), ("password", str), ("page_size", int), ("auth", str),
                      ("secret_key", str)):
        if key in dash:
            setattr(d, key, _expect(dash[key], kind, f"{src} dashboard.{key}"))
    if "idle_timeout" in dash:
        d.idle_timeout = parse_duration(dash["idle_timeout"], f"{src} dashboard.idle_timeout") or None
    if not d.path.startswith("/"):
        raise ConfigError(f'{src} dashboard.path: must start with "/", e.g. "/schedule-fire"')
    if d.auth is not None:
        d.auth = d.auth.lower()
        if d.auth not in _AUTH_MODES:
            raise ConfigError(f"{src} dashboard.auth: use one of {', '.join(_AUTH_MODES)}")
    if d.username and not d.password:
        raise ConfigError(f"{src} dashboard.password: required when dashboard.username is set")
    if "users" in dash:
        users = _expect(dash["users"], dict, f"{src} [dashboard.users]")
        for name, secret in users.items():
            _expect(secret, str, f"{src} dashboard.users.{name}.password")
        d.users = dict(users)
    if "oauth" in dash:
        oauth = _expect(dash["oauth"], dict, f"{src} [dashboard.oauth]")
        where = f"{src} [dashboard.oauth]"
        _check_keys(oauth, _OAUTH_KEYS, where)
        for key in ("scopes", "allowed_emails", "allowed_domains", "allowed_users"):
            if key in oauth:
                oauth[key] = _str_list(oauth[key], f"{where}.{key}")
        if "allow_any_user" in oauth:
            _expect(oauth["allow_any_user"], bool, f"{where}.allow_any_user")
        for key in _OAUTH_DURATION_KEYS:
            if key in oauth:
                oauth[key] = parse_duration(oauth[key], f"{where}.{key}")
        for key in _OAUTH_KEYS - {"scopes", "allowed_emails", "allowed_domains", "allowed_users", "allow_any_user",
                                  *_OAUTH_DURATION_KEYS}:
            if key in oauth:
                _expect(oauth[key], str, f"{where}.{key}")
        d.oauth = dict(oauth)
    if "routes" in dash:
        routes = _expect(dash["routes"], dict, f"{src} [dashboard.routes]")
        # `false` switches an optional page off.
        d.routes = {k: (None if v is False else v) for k, v in routes.items()}
        from .dashboard.routing import Router

        try:
            Router(d.routes)
        except ValueError as e:
            raise ConfigError(f"{src} [dashboard.routes]: {e}") from None
    mode = d.auth_mode
    if mode == "basic" and not (d.username or d.users):
        raise ConfigError(f'{src} [dashboard]: auth = "basic" needs username/password or a [dashboard.users] table')
    if mode == "oauth":
        if not d.oauth:
            raise ConfigError(f'{src} [dashboard]: auth = "oauth" needs a [dashboard.oauth] table')
        try:
            _validate_oauth(d.oauth)
        except ValueError as e:
            raise ConfigError(f"{src} [dashboard.oauth]: {e}") from None

    # [logging]
    log = _expect(raw.get("logging", {}), dict, f"{src} [logging]")
    _check_keys(log, {"level", "format"}, f"{src} [logging]")
    if "level" in log:
        level = _expect(log["level"], str, f"{src} logging.level").upper()
        if level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            raise ConfigError(f"{src} logging.level: use DEBUG, INFO, WARNING, ERROR or CRITICAL")
        settings.log_level = level
    if "format" in log:
        settings.log_format = _expect(log["format"], str, f"{src} logging.format")

    # [security]
    security = _expect(raw.get("security", {}), dict, f"{src} [security]")
    _check_keys(security, {"allowed_job_modules", "allow_stdlib_jobs", "blocked_job_modules"}, f"{src} [security]")
    if "allowed_job_modules" in security:
        settings.security.allowed_job_modules = _str_list(security["allowed_job_modules"],
                                                          f"{src} security.allowed_job_modules")
    if "allow_stdlib_jobs" in security:
        settings.security.allow_stdlib_jobs = _expect(security["allow_stdlib_jobs"], bool,
                                                      f"{src} security.allow_stdlib_jobs")
    if "blocked_job_modules" in security:
        settings.security.blocked_job_modules = _str_list(security["blocked_job_modules"],
                                                          f"{src} security.blocked_job_modules")

    # [[recurring]]
    recurring = _expect(raw.get("recurring", []), list, f"{src} [[recurring]]")
    seen = set()
    for i, item in enumerate(recurring):
        where = f"{src} recurring[{i}]"
        _expect(item, dict, where)
        _check_keys(item, {"id", "job", "cron", "time_zone", "queue", "args", "kwargs"}, where)
        for required in ("id", "job", "cron"):
            if required not in item:
                raise ConfigError(f'{where}: "{required}" is required')
        from .cron import CronExpression

        try:
            CronExpression(_expect(item["cron"], str, f"{where}.cron"))
        except ValueError as e:
            raise ConfigError(f"{where}.cron: {e}") from None
        rid = _expect(item["id"], str, f"{where}.id")
        if rid in seen:
            raise ConfigError(f"{where}: duplicate recurring job id {rid!r}")
        seen.add(rid)
        settings.recurring.append(RecurringSettings(
            id=rid, job=_expect(item["job"], str, f"{where}.job"), cron=item["cron"],
            time_zone=item.get("time_zone"), queue=item.get("queue"),
            args=list(_expect(item.get("args", []), list, f"{where}.args")),
            kwargs=dict(_expect(item.get("kwargs", {}), dict, f"{where}.kwargs")),
        ))
    return settings


def _parse_storage(raw: Any, src: str, environ: Mapping[str, str]) -> Tuple[Dict[str, StorageProfile], StorageProfile]:
    raw = _expect(raw, dict, f"{src} [storage]")
    profiles: Dict[str, StorageProfile] = {}
    for key, value in raw.items():
        if key == "use":
            continue
        if not isinstance(value, dict):
            hint = ' - storage settings go in a named table like [storage.postgres]'
            raise ConfigError(f'{src} [storage]: unknown setting "{key}"{hint}')
        where = f"{src} [storage.{key}]"
        type_name = _TYPE_ALIASES.get(str(value.get("type", key)).lower())
        if type_name is None:
            raise ConfigError(f'{where}: unknown type {value.get("type", key)!r}; use one of '
                              f'sqlite, postgres, mysql, sqlserver, redis, memory (or set type = "...")')
        _check_keys(value, _STORAGE_KEYS[type_name] | _COMMON_STORAGE_KEYS, where)
        options = {k: v for k, v in value.items() if k != "type"}
        for k, v in options.items():
            kind = _STORAGE_TYPES_OF.get(k)
            if kind is not None:
                _expect(v, kind, f"{where}.{k}")
        for dur in ("job_expiration", "invisibility_timeout"):
            if dur in options:
                options[dur] = parse_duration(options[dur], f"{where}.{dur}")
        profiles[key] = StorageProfile(key, type_name, options)

    # Environment beats the file: a URL wins outright, a profile name picks from the file.
    url = environ.get("SCHEDULEFIRE_DB")
    if url:
        return profiles, _profile_from_url(url)
    use = environ.get("SCHEDULEFIRE_STORAGE") or raw.get("use")
    if use is None:
        if not profiles:
            return profiles, StorageProfile("sqlite", "sqlite", {"path": "schedulefire.db"})
        if len(profiles) == 1:
            return profiles, next(iter(profiles.values()))
        raise ConfigError(f'{src} [storage]: {len(profiles)} storages are defined ({", ".join(profiles)}); '
                          f'choose one with use = "name"')
    _expect(use, str, f"{src} storage.use")
    if use not in profiles:
        if use.lower() in _TYPE_ALIASES and not profiles:
            return profiles, StorageProfile(use, _TYPE_ALIASES[use.lower()], {})
        raise ConfigError(f'{src} storage.use = "{use}", but there is no [storage.{use}] section '
                          f'(defined: {", ".join(profiles) or "none"})')
    return profiles, profiles[use]


def _profile_from_url(url: str) -> StorageProfile:
    lowered = url.lower()
    for prefix, kind, key in (
        (("postgresql://", "postgres://"), "postgres", "url"),
        (("redis://", "rediss://", "unix://"), "redis", "url"),
        (("mysql://", "mysql+pymysql://", "mariadb://"), "mysql", "url"),
        (("mssql://", "sqlserver://", "mssql+pyodbc://", "driver="), "sqlserver", "connection"),
    ):
        if lowered.startswith(prefix):
            return StorageProfile("SCHEDULEFIRE_DB", kind, {key: url})
    if lowered.startswith("memory://"):
        return StorageProfile("SCHEDULEFIRE_DB", "memory", {})
    path = url[len("sqlite:///"):] if lowered.startswith("sqlite:///") else url
    return StorageProfile("SCHEDULEFIRE_DB", "sqlite", {"path": path})


TEMPLATE = '''# scheduleFire configuration
# Durations accept seconds or strings: "500ms", "15s", "5m", "1h30m", "1d".
# Any value can read environment variables: "${DB_PASSWORD}" or "${DB_HOST:-localhost}".
# Precedence: code / CLI flags > SCHEDULEFIRE_DB / SCHEDULEFIRE_STORAGE env vars > this file > defaults.

[storage]
# Which storage below is active. Override per environment with SCHEDULEFIRE_STORAGE=postgres.
use = "sqlite"

[storage.sqlite]
path = "data/schedulefire.db"            # folders are created automatically

[storage.postgres]
host = "${PG_HOST:-localhost}"
port = 5432
user = "app"
password = "${PG_PASSWORD:-}"
database = "app"                          # created automatically if missing (needs CREATEDB)
schema = "schedulefire"
# url = "postgresql://app:${PG_PASSWORD}@localhost:5432/app"   # alternative to host/user/...

[storage.mysql]
host = "${MYSQL_HOST:-localhost}"
port = 3306
user = "app"
password = "${MYSQL_PASSWORD:-}"
database = "app"
table_prefix = "schedulefire_"

[storage.sqlserver]
host = "${MSSQL_HOST:-localhost}"
port = 1433
user = "app"                              # leave out user/password for Windows authentication
password = "${MSSQL_PASSWORD:-}"
database = "app"
schema = "schedulefire"
trust_server_certificate = true
# connection = "Driver={ODBC Driver 18 for SQL Server};Server=...;Database=...;..."  # raw ODBC string

[storage.redis]
host = "${REDIS_HOST:-localhost}"
port = 6379
password = "${REDIS_PASSWORD:-}"
db = 0

[storage.memory]                          # no database: one process only, lost on exit

[jobs]
expiration = "1d"                         # how long succeeded/deleted jobs are kept
invisibility_timeout = "30m"              # when a crashed worker's job becomes available again

[retry]
attempts = 10
# delays = ["10s", "1m", "5m"]           # default: Hangfire's growing back-off
on_attempts_exceeded = "fail"             # "fail" (keep in Failed) or "delete"
# except_on = ["builtins.ValueError"]    # never retry these exception types

[server]
# name = "worker-01"
workers = 10                              # default: min(cpu_count * 5, 20)
queues = ["critical", "default"]          # highest priority first
imports = []                              # modules to import at startup, e.g. ["myapp.jobs"]
schedule_polling_interval = "15s"
shutdown_timeout = "15s"

[dashboard]
enabled = true                            # `schedulefire server` also serves the dashboard
host = "127.0.0.1"                        # "0.0.0.0" to accept connections from other machines
port = 8080
path = "/schedule-fire"                   # where the dashboard lives; "/" for the site root
title = "scheduleFire"
read_only = false
auth = "none"                             # "none" (anyone who can reach it), "local", "basic" or "oauth"
idle_timeout = "5m"                       # sign out an inactive basic/oauth session; "0" disables it
# secret_key = "${DASHBOARD_SECRET_KEY}"  # signs basic-auth sessions; >= 32 chars, same for every process

# auth = "basic": a proper sign-in form (not the browser's native popup). One or more users;
# store hashes from `schedulefire hash-password`, not plain passwords.
# [dashboard.users]
# admin = "pbkdf2_sha256$600000$..."
# ops = "${OPS_DASHBOARD_PASSWORD}"

# auth = "oauth": sign in with Google, Microsoft, GitHub or any OpenID Connect provider.
# Register https://<host>/schedule-fire/auth/callback as the redirect URI at the provider.
# [dashboard.oauth]
# provider = "google"                    # google | microsoft | github | oidc (Okta, Auth0, Keycloak...)
# client_id = "${OAUTH_CLIENT_ID}"
# client_secret = "${OAUTH_CLIENT_SECRET}"
# secret_key = "${DASHBOARD_SECRET_KEY}" # >= 32 chars, same for every dashboard process
# allowed_domains = ["example.com"]      # and/or allowed_emails / allowed_users
# tenant = "your-tenant-id"              # microsoft
# issuer = "https://login.example.com"   # oidc
# session_lifetime = "8h"                # absolute cap regardless of activity
# idle_timeout = "5m"                    # overrides the top-level [dashboard] idle_timeout

# Rename pages or switch optional ones off (recurring, servers, queues, search):
# [dashboard.routes]
# recurring = "/schedules"
# job = "/jobs/view/{id}"
# servers = false

[logging]
level = "INFO"

[security]
# A job is stored as "module + function name", so anyone who can write to the job database
# could otherwise make workers call any importable function. Standard-library functions
# (os.system, subprocess.run, ...) are always refused unless allow_stdlib_jobs = true.
# For production, list the only modules that contain your jobs:
# allowed_job_modules = ["myapp.jobs", "billing"]
allow_stdlib_jobs = false

# Recurring jobs created/updated when the server starts:
# [[recurring]]
# id = "nightly-report"
# job = "myapp.jobs:build_report"
# cron = "30 2 * * *"
# time_zone = "UTC"
# args = []
'''

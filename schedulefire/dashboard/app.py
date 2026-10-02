"""The dashboard WSGI application."""

from __future__ import annotations

import hmac
import logging
import re
import secrets
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union
from urllib.parse import parse_qs, quote, urlencode, urlsplit

from .. import __version__, config
from ..client import BackgroundJobClient, RecurringJobManager
from ..server import MAX_WORKERS, _validate_queues, update_server
from ..states import ALL_STATES, AWAITING, DELETED, ENQUEUED, FAILED, PROCESSING, SCHEDULED, SUCCEEDED
from ..storage.base import JobRecord, JobStorage, RecurringJobRecord
from . import assets
from .assets import icon
from .auth import (
    BasicAuth,
    DashboardAuth,
    OAuth,
    OAuthError,
    Request,
    User,
    format_duration,
    resolve_auth,
    safe_next,
)
from .routing import CustomPage, Router
from .components import (
    COMMON_TIME_ZONES,
    DAYS_OF_WEEK,
    STATE_META,
    absolute,
    chart_table,
    compact,
    describe_cron,
    duration,
    empty_state,
    h,
    job_cell,
    parse_iso,
    pill,
    pretty_json,
    schedule_builder_state,
    span_text,
    state_dot,
    throughput_chart,
    when,
)

logger = logging.getLogger("schedulefire.dashboard")

Response = Tuple[str, List[Tuple[str, str]], bytes]

_JOB_ID_PATH = re.compile(r"^/job/(\d+)$")
_STATE_PATH = re.compile(r"^/jobs/([a-z]+)$")
_PAGE_SIZES = (10, 20, 50, 100)
_STATE_BY_SLUG = {s.lower(): s for s in ALL_STATES}
_CSRF_COOKIE = "schedulefire_csrf"
_CSRF_TOKEN = re.compile(r"^[A-Za-z0-9_-]{32,64}$")


@dataclass
class PageContext:
    """What a custom page's render function receives."""

    storage: JobStorage
    query: Dict[str, List[str]]
    user: Optional[User]
    #: ``url("/job/12")`` -> the public URL of a dashboard page (follows custom routes).
    url: Callable[..., str]
    #: HTML-escape helper - escape everything that isn't your own markup.
    escape: Callable[[str], str] = field(default=h)


def _plural(n: int, word: str) -> str:
    return f"{n:,} {word}{'' if n == 1 else 's'}"


_TIME_RE = re.compile(r"^([01]?\d|2[0-3]):([0-5]\d)$")


def _parse_time(raw: str) -> Tuple[int, int]:
    m = _TIME_RE.match(raw.strip())
    if not m:
        raise ValueError(f"{raw!r} is not a valid time (HH:MM)")
    return int(m.group(1)), int(m.group(2))


def _cron_from_builder(form: dict) -> str:
    """Build a cron expression from the recurring-job schedule builder's fields - the
    friendly alternative to typing one directly. Raises ValueError with a message fit to
    show the user. ``frequency=custom`` (the default, so plain ``cron=...`` posts still work)
    uses the ``cron`` field as-is."""

    def field(name: str, default: str = "") -> str:
        return form.get(name, [default])[0].strip()

    frequency = field("frequency", "custom")
    if frequency == "minutes":
        n = field("interval_minutes")
        if not n.isdigit() or not 1 <= int(n) <= 59:
            raise ValueError("Enter a number of minutes between 1 and 59.")
        return f"*/{int(n)} * * * *"
    if frequency == "hourly":
        interval, minute = field("interval_hours", "1"), field("minute", "0")
        if not interval.isdigit() or not 1 <= int(interval) <= 23:
            raise ValueError("Enter a number of hours between 1 and 23.")
        if not minute.isdigit() or not 0 <= int(minute) <= 59:
            raise ValueError("Enter a minute between 0 and 59.")
        prefix = "*" if int(interval) == 1 else f"*/{int(interval)}"
        return f"{int(minute)} {prefix} * * *"
    if frequency == "daily":
        hour, minute = _parse_time(field("time_daily"))
        return f"{minute} {hour} * * *"
    if frequency == "weekly":
        hour, minute = _parse_time(field("time_weekly"))
        dow = field("day_of_week", "0")
        if not dow.isdigit() or not 0 <= int(dow) <= 6:
            raise ValueError("Choose a day of the week.")
        return f"{minute} {hour} * * {int(dow)}"
    if frequency == "monthly":
        hour, minute = _parse_time(field("time_monthly"))
        dom = field("day_of_month")
        if not dom.isdigit() or not 1 <= int(dom) <= 31:
            raise ValueError("Enter a day of the month between 1 and 31.")
        return f"{minute} {hour} {int(dom)} * *"
    cron = field("cron")
    if not cron:
        raise ValueError("Enter a cron expression.")
    return cron


class DashboardApp:
    def __init__(
        self,
        storage: Optional[JobStorage] = None,
        *,
        title: str = "scheduleFire",
        auth: Union[None, str, DashboardAuth] = None,
        routes: Optional[Dict[str, Union[str, None, bool]]] = None,
        authorization: Optional[Callable[[dict], bool]] = None,
        basic_auth: Optional[Tuple[str, str]] = None,
        read_only: bool = False,
        page_size: int = 20,
        stats_poll_interval: float = 5.0,
    ):
        """
        :param auth: who may use the dashboard -
            ``None`` / ``"none"``: anyone who can reach it (no authentication);
            ``"local"``: only browsers on this machine;
            ``BasicAuth(...)``: HTTP Basic users; ``OAuth(...)``: sign in with Google, Microsoft,
            GitHub or any OpenID Connect provider.
        :param routes: rename pages or switch optional ones off, e.g.
            ``{"recurring": "/schedules", "job": "/jobs/view/{id}", "servers": None}``.
            See ``schedulefire.dashboard.routing.CANONICAL_ROUTES`` for the page names.
        :param authorization: extra ``callable(environ) -> bool`` check applied after ``auth``.
        :param basic_auth: shortcut for ``auth=BasicAuth(username=..., password=...)``.
        :param read_only: hide and reject all actions (requeue, delete, trigger).
        :param stats_poll_interval: seconds between the dashboard's live-refresh polls
            while "Live" is on (Hangfire's ``StatsPollingInterval``).
        """
        self._storage = storage
        self.title = title
        self.auth = resolve_auth(auth, basic_auth)
        self.router = Router(routes)
        self.authorization = authorization
        self.read_only = read_only
        self.page_size = page_size
        self.stats_poll_interval = stats_poll_interval
        self._request = threading.local()  # per-request state (mount prefix, user, CSRF token)

    @property
    def basic_auth(self):  # kept for compatibility: truthy when a login is required
        return None if self.auth.name in ("none", "local") else self.auth

    def add_page(self, path: str, title: str, render: Callable[[PageContext], str], *, nav: bool = True,
                 icon: str = "overview") -> None:
        """Add a custom page shown inside the dashboard layout (and in the sidebar if ``nav``).

        ``render(context)`` returns the page's HTML. Escape any data you include with
        ``context.escape``. The page is protected by the dashboard's authentication.
        """
        self.router.add_page(CustomPage(path=path.rstrip("/") or "/", title=title, render=render, nav=nav,
                                        icon=icon))

    def page(self, path: str, title: str, **options):
        """Decorator form of :meth:`add_page`."""
        def decorator(render):
            self.add_page(path, title, render, **options)
            return render
        return decorator

    @property
    def storage(self) -> JobStorage:
        return self._storage or config.get_storage()

    # ------------------------------------------------------------------ WSGI

    def __call__(self, environ: dict, start_response) -> Iterable[bytes]:
        try:
            status, headers, body = self._handle(environ)
        except Exception:
            logger.exception("Dashboard request failed")
            status, headers, body = "500 Internal Server Error", [("Content-Type", "text/plain")], b"Internal Server Error"
        # A refreshed idle-timeout session cookie (if the auth layer touched one while
        # authenticating) rides on every response, not just ones built by _page(). It goes
        # BEFORE the response's own headers so e.g. /auth/logout's explicit Set-Cookie clear
        # (added after authenticate() already touched/refreshed the same cookie for this
        # same POST) is the one a browser ends up applying, not the other way round.
        req = getattr(self._request, "req", None)
        if req is not None and req.extra_headers:
            headers = [*req.extra_headers, *headers]
        names = {k.lower() for k, _ in headers}
        headers = [("Content-Length", str(len(body))), *headers]
        if "content-type" not in names:
            headers.append(("Content-Type", "text/html; charset=utf-8"))
        if "cache-control" not in names:
            headers.append(("Cache-Control", "no-store"))
        headers += [("X-Frame-Options", "DENY"), ("X-Content-Type-Options", "nosniff"),
                    ("Referrer-Policy", "same-origin"), ("Content-Security-Policy", assets.CSP),
                    ("Cross-Origin-Opener-Policy", "same-origin"), ("Cross-Origin-Resource-Policy", "same-origin")]
        start_response(status, headers)
        if environ.get("REQUEST_METHOD", "GET").upper() == "HEAD":
            return [b""]
        return [body]

    def _handle(self, environ: dict) -> Response:
        req = Request(environ)
        query = req.query
        self._request.base = req.base
        self._request.req = req
        self._request.user = None
        self._request.notice = (query.get("notice") or [""])[0][:200]
        self._request.level = (query.get("level") or [""])[0]
        # CSRF protection with the double-submit cookie pattern: the token lives in a cookie
        # and is repeated in every form. No server-side state, so it works across processes
        # (gunicorn -w 4), load-balanced instances and restarts.
        token = req.cookies.get(_CSRF_COOKIE)
        self._request.csrf_cookie = token if token and _CSRF_TOKEN.match(token) else None
        self._request.csrf = self._request.csrf_cookie or secrets.token_urlsafe(32)
        self._request.https = req.is_https

        path = self.router.to_canonical(req.path)
        # Static files are public (they contain no data), so the sign-in pages can use them.
        if path == "/assets/app.css":
            return self._asset(assets.CSS, "text/css; charset=utf-8")
        if path == "/assets/app.js":
            return self._asset(assets.JS, "application/javascript; charset=utf-8")

        # Sign-in endpoints must be reachable before authentication. /auth/login also takes the
        # sign-in form's POST (BasicAuth); /auth/callback is GET-only (the provider redirects here).
        if path == "/auth/login" and req.method in ("GET", "HEAD", "POST"):
            return self._auth_endpoint(req, path)
        if path == "/auth/callback" and req.method in ("GET", "HEAD"):
            return self._auth_endpoint(req, path)

        result = self.auth.authenticate(req, login_url=self._url("/auth/login"))
        if isinstance(result, tuple):
            return result
        self._request.user = result
        if self.authorization is not None and not self.authorization(environ):
            return "403 Forbidden", [("Content-Type", "text/plain")], b"Forbidden."

        if req.method == "POST":
            if not self._same_origin(environ):
                return "403 Forbidden", [("Content-Type", "text/plain")], b"Cross-site request refused."
            return self._post(environ, path or "")
        if req.method not in ("GET", "HEAD"):
            return "405 Method Not Allowed", [("Allow", "GET, HEAD, POST")], b"Method Not Allowed"

        if path == "/":
            return self._overview(query)
        if path is not None:
            m = _STATE_PATH.match(path)
            if m and m.group(1) in _STATE_BY_SLUG:
                return self._job_list(_STATE_BY_SLUG[m.group(1)], query)
            m = _JOB_ID_PATH.match(path)
            if m:
                return self._job_details(m.group(1))
            if path == "/recurring":
                return self._recurring()
            if path == "/servers":
                return self._servers()
            if path == "/queues":
                return self._queues()
            if path == "/search":
                return self._search(query)
            if path in self.router.pages:
                return self._custom_page(self.router.pages[path], query)
        return self._page("Not found", empty_state("search", "Page not found",
                                                    "The page you are looking for doesn't exist."),
                          crumbs=[("Not found", None)], status="404 Not Found")

    def _auth_endpoint(self, req: Request, path: str) -> Response:
        if isinstance(self.auth, OAuth):
            callback = self.router.to_public("/auth/callback")
            try:
                if path == "/auth/login":
                    return self.auth.login(req, callback)
                user, response = self.auth.callback(req, callback)
            except OAuthError as e:
                logger.warning("Dashboard sign-in failed: %s", e)
                return self._bare_page("Sign-in failed", h(str(e)), "Try again", self._url("/auth/login"),
                                       status="400 Bad Request")
            if user is None:
                status, headers, reason = response
                return self._bare_page("Access denied", h(reason.decode("utf-8")), "Sign in with another account",
                                       self._url("/auth/login"), status=status, headers=headers)
            return response

        if isinstance(self.auth, BasicAuth) and path == "/auth/login":
            if req.method == "POST":
                return self._basic_login_post(req)
            idle = (req.query.get("reason") or [""])[0] == "idle"
            next_path = safe_next((req.query.get("next") or [""])[0])
            return self._login_page(next_path, idle=idle)

        return "303 See Other", [("Location", self._url("/"))], b""

    def _basic_login_post(self, req: Request) -> Response:
        if not self._same_origin(req.environ):
            return "403 Forbidden", [("Content-Type", "text/plain")], b"Cross-site request refused."
        try:
            length = int(req.environ.get("CONTENT_LENGTH") or 0)
        except ValueError:
            length = 0
        length = max(0, min(length, 1_000_000))
        form = parse_qs(req.environ["wsgi.input"].read(length).decode("utf-8", "replace") if length else "")
        next_path = safe_next(form.get("next", [""])[0])
        if not self._request.csrf_cookie or not hmac.compare_digest(form.get("csrf", [""])[0], self._request.csrf_cookie):
            return "403 Forbidden", [("Content-Type", "text/plain")], \
                b"Invalid or missing CSRF token. Reload the page and try again."
        result = self.auth.verify_login(req, form.get("username", [""])[0], form.get("password", [""])[0])
        if isinstance(result, tuple):  # a Response: too many recent failures
            return result
        if result is None:
            return self._login_page(next_path, error=True, status="401 Unauthorized")
        logger.info("Dashboard sign-in: %s (basic)", result.display)
        session_cookie = self.auth.start_session(req, result.id)
        return "303 See Other", [("Location", self._request.base + next_path), session_cookie], b""

    def _csrf_headers(self) -> List[Tuple[str, str]]:
        if self._request.csrf_cookie is None:
            return [("Set-Cookie", f"{_CSRF_COOKIE}={self._request.csrf}; Path={self._request.base or '/'}; "
                                   f"HttpOnly; SameSite=Strict" + ("; Secure" if self._request.https else ""))]
        return []

    def _login_page(self, next_path: str, *, error: bool = False, idle: bool = False,
                    status: str = "200 OK") -> Response:
        """A proper sign-in form for BasicAuth (not the browser's native Basic-auth popup)."""
        notice = ""
        if idle and self.auth.idle_timeout:
            notice = (f'<div class="notice" role="status">{icon("scheduled")}'
                      f'<span>You were signed out after {h(format_duration(self.auth.idle_timeout))} '
                      f'of inactivity.</span></div>')
        elif error:
            notice = (f'<div class="notice error" role="alert">{icon("failed")}'
                      f'<span>Incorrect username or password.</span></div>')
        doc = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Sign in · {h(self.title)}</title>
<script>{assets.THEME_BOOT}</script>
<link rel="stylesheet" href="{h(self._url('/assets/app.css', v=assets.CSS_VERSION))}">
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='8' fill='%23e8590c'/%3E%3Cpath d='M12.5 18.5A2.5 2.5 0 0 0 15 16c0-1.4-.5-2-1-3-1-2.1-.2-4 2-6 .5 2.5 2 4.9 4 6.5 2 1.6 3 3.5 3 5.5a7 7 0 1 1-14 0c0-1.2.4-2.3 1-3a2.5 2.5 0 0 0 2.5 2.5z' fill='none' stroke='white' stroke-width='2' stroke-linejoin='round'/%3E%3C/svg%3E"></head>
<body><main class="content" style="max-width:380px;padding-top:12vh">
<div class="brand" style="justify-content:center;margin-bottom:18px"><span class="brand-mark">{icon('flame')}</span>{h(self.title)}</div>
<section class="card"><div class="card-body">
{notice}
<form method="post" action="{h(self._url('/auth/login'))}">
<input type="hidden" name="csrf" value="{h(self._request.csrf)}">
<input type="hidden" name="next" value="{h(next_path)}">
<label class="field"><span>Username</span><input name="username" autocomplete="username" required autofocus></label>
<label class="field" style="margin-top:12px"><span>Password</span>
<input type="password" name="password" autocomplete="current-password" required></label>
<button class="btn primary" type="submit" style="margin-top:16px;width:100%;justify-content:center">Sign in</button>
</form>
</div></section>
</main></body></html>"""
        return status, self._csrf_headers(), doc.encode("utf-8")

    def _bare_page(self, title: str, message_html: str, link_label: str, link: str, *,
                   status: str = "200 OK", headers: Sequence[Tuple[str, str]] = ()) -> Response:
        """A minimal page outside the dashboard layout (sign-in errors, signed out)."""
        doc = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>{h(title)} · {h(self.title)}</title>
<script>{assets.THEME_BOOT}</script>
<link rel="stylesheet" href="{h(self._url('/assets/app.css', v=assets.CSS_VERSION))}"></head>
<body><main class="content" style="max-width:480px;padding-top:12vh">
<section class="card"><div class="empty"><div class="ring">{icon('flame')}</div><h3>{h(title)}</h3>
<p>{message_html}</p><p style="margin-top:16px"><a class="btn primary" href="{h(link)}">{h(link_label)}</a></p>
</div></section></main></body></html>"""
        return status, list(headers), doc.encode("utf-8")

    @staticmethod
    def _asset(text: str, content_type: str) -> Response:
        return "200 OK", [("Content-Type", content_type), ("Cache-Control", "public, max-age=86400")], \
            text.encode("utf-8")

    @staticmethod
    def _same_origin(environ: dict) -> bool:
        """Browsers send Origin (or at least Referer) on form posts; it must be this host."""
        source = environ.get("HTTP_ORIGIN") or environ.get("HTTP_REFERER") or ""
        if not source:
            return True  # non-browser client; the CSRF token still applies
        if source == "null":
            return False
        netloc = urlsplit(source).netloc.lower()
        hosts = {environ.get("HTTP_HOST", "").lower(), environ.get("HTTP_X_FORWARDED_HOST", "").split(",")[0].strip().lower()}
        server = f"{environ.get('SERVER_NAME', '')}:{environ.get('SERVER_PORT', '')}".lower()
        return bool(netloc) and (netloc in hosts or netloc == server)

    def _url(self, path: str, **params) -> str:
        """Public URL of a canonical dashboard path (follows custom routes and the mount prefix)."""
        url = self._request.base + self.router.to_public(path)
        return url + ("?" + urlencode(params) if params else "")

    def _enabled(self, page: str) -> bool:
        return self.router.enabled(page)

    # --------------------------------------------------------------- actions

    def _post(self, environ: dict, path: str) -> Response:
        if self.read_only:
            return "403 Forbidden", [("Content-Type", "text/plain")], b"The dashboard is read-only."
        try:
            length = int(environ.get("CONTENT_LENGTH") or 0)
        except ValueError:
            length = 0
        # Clamp: a negative CONTENT_LENGTH would otherwise mean "read everything".
        length = max(0, min(length, 1_000_000))
        form = parse_qs(environ["wsgi.input"].read(length).decode("utf-8", "replace") if length else "")
        cookie_token = self._request.csrf_cookie
        if not cookie_token or not hmac.compare_digest(form.get("csrf", [""])[0], cookie_token):
            return "403 Forbidden", [("Content-Type", "text/plain")], \
                b"Invalid or missing CSRF token. Reload the page and try again."

        if path == "/auth/logout":
            user = self._request.user
            headers = [self.auth.logout(self._request.req)] if self.auth.supports_logout else []
            if user is not None:
                logger.info("Dashboard sign-out: %s", user.display)
            return self._bare_page("Signed out", "You have signed out of the dashboard.", "Sign in again",
                                   self._url("/auth/login"), headers=headers)
        if path == "/actions/servers/update":
            return self._update_server(form)
        if path == "/actions/recurring/update":
            return self._update_recurring(form)
        if path == "/actions/jobs/reschedule":
            return self._reschedule_job(form)

        ids = list(dict.fromkeys(form.get("id", [])))[:1000]  # dedupe, keep order, bound the work
        client = BackgroundJobClient(self.storage)
        recurring = RecurringJobManager(self.storage)
        actions = {
            "/actions/jobs/requeue": (client.requeue, "job", "requeued"),
            "/actions/jobs/delete": (client.delete, "job", "deleted"),
            "/actions/recurring/trigger": (recurring.trigger, "recurring job", "triggered"),
            "/actions/recurring/delete": (recurring.remove_if_exists, "recurring job", "removed"),
        }
        if path not in actions:
            return "404 Not Found", [("Content-Type", "text/plain")], b"Not Found"
        action, noun, verb = actions[path]
        done = sum(1 for item in ids if action(item))
        user = self._request.user
        logger.info("Dashboard: %s %s %s (%s)", user.display if user else "anonymous user", verb,
                    _plural(done, noun), ", ".join(ids[:20]) + ("..." if len(ids) > 20 else ""))
        notice = f"{_plural(done, noun)} {verb}." if done else f"No {noun}s were {verb}."
        return self._redirect(form.get("next", [""])[0], notice)

    def _redirect(self, next_url: str, notice: str, error: bool = False) -> Response:
        next_url = self._request.base + self.router.to_public(safe_next(next_url))
        separator = "&" if "?" in next_url else "?"
        location = f"{next_url}{separator}notice={quote(notice)}" + ("&level=error" if error else "")
        return "303 See Other", [("Location", location)], b""

    def _update_server(self, form: dict) -> Response:
        server_id = form.get("id", [""])[0]
        server = self.storage.get_server(server_id)
        if server is None:
            return self._redirect("/servers", "That server is no longer running.", error=True)
        try:
            workers = form.get("workers", [""])[0].strip()
            queues = form.get("queues", [""])[0].strip()
            worker_count = int(workers) if workers else None
            if worker_count is not None and worker_count == server.worker_count and not (server.requested or {}).get("worker_count"):
                worker_count = None
            queue_list = _validate_queues(queues) if queues else None
            if queue_list is not None and queue_list == server.queues and not (server.requested or {}).get("queues"):
                queue_list = None
            if worker_count is None and queue_list is None:
                return self._redirect("/servers", f"No changes for {server.name}.")
            update_server(server_id, worker_count=worker_count, queues=queue_list, storage=self.storage)
        except ValueError as e:
            return self._redirect("/servers", f"Invalid settings: {e}", error=True)
        return self._redirect("/servers", f"Update sent to {server.name}. It applies within a few seconds.")

    def _update_recurring(self, form: dict) -> Response:
        recurring_id = form.get("id", [""])[0]
        record = self.storage.get_recurring(recurring_id)
        if record is None:
            return self._redirect("/recurring", "That recurring job no longer exists.", error=True)
        try:
            cron = _cron_from_builder(form)
        except ValueError as e:
            return self._redirect("/recurring", f"Could not update {recurring_id}: {e}", error=True)
        time_zone = form.get("time_zone", [""])[0].strip() or record.time_zone
        queue = form.get("queue", [""])[0].strip() or record.queue
        if (cron, time_zone, queue) == (record.cron, record.time_zone, record.queue):
            return self._redirect("/recurring", f"No changes for {recurring_id}.")
        try:
            RecurringJobManager(self.storage).add_or_update(recurring_id, record.job, cron, time_zone=time_zone,
                                                            queue=queue)
        except ValueError as e:
            return self._redirect("/recurring", f"Could not update {recurring_id}: {e}", error=True)
        return self._redirect("/recurring", f"Updated the schedule for {recurring_id}.")

    def _reschedule_job(self, form: dict) -> Response:
        job_id = form.get("id", [""])[0]
        next_path = form.get("next", [""])[0] or f"/job/{job_id}"
        mode = form.get("when_mode", ["in"])[0]
        client = BackgroundJobClient(self.storage)
        if mode == "at":
            raw = form.get("at", [""])[0].strip()
            try:
                when_dt = datetime.strptime(raw, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)
            except ValueError:
                return self._redirect(next_path, "Enter a valid date and time.", error=True)
            if when_dt <= datetime.now(timezone.utc):
                return self._redirect(next_path, "Pick a time in the future.", error=True)
            if not client.reschedule(job_id, when_dt, from_state=SCHEDULED):
                return self._redirect(next_path, "That job is no longer Scheduled.", error=True)
            return self._redirect(next_path, f"Rescheduled to run at {when_dt.strftime('%Y-%m-%d %H:%M')} UTC.")
        try:
            amount = float(form.get("amount", [""])[0])
        except (ValueError, TypeError):
            return self._redirect(next_path, "Enter a number to reschedule by.", error=True)
        if amount <= 0:
            return self._redirect(next_path, "The delay must be greater than zero.", error=True)
        unit = form.get("unit", ["minutes"])[0]
        seconds = amount * {"minutes": 60, "hours": 3600, "days": 86400}.get(unit, 60)
        if not client.reschedule(job_id, seconds, from_state=SCHEDULED):
            return self._redirect(next_path, "That job is no longer Scheduled.", error=True)
        return self._redirect(next_path, f"Rescheduled to run in {amount:g} {unit}.")

    def _hidden(self, next_path: str) -> str:
        return (f'<input type="hidden" name="csrf" value="{h(self._request.csrf)}">'
                f'<input type="hidden" name="next" value="{h(next_path)}">')

    def _row_action(self, action: str, item_id: str, next_path: str, label: str, icon_name: str,
                    danger: bool = False, confirm: Optional[str] = None) -> str:
        if self.read_only:
            return ""
        confirm_attr = f' data-confirm="{h(confirm)}"' if confirm else ""
        return (f'<form method="post" action="{h(self._url(action))}" style="display:inline">{self._hidden(next_path)}'
                f'<input type="hidden" name="id" value="{h(item_id)}">'
                f'<button class="btn sm{" danger" if danger else ""}"{confirm_attr}>{icon(icon_name)}{h(label)}</button></form>')

    def _bulk_toolbar(self, next_path: str, buttons: Sequence[Tuple[str, str, str, bool, Optional[str]]]) -> str:
        """``buttons``: (action, label, icon, danger, confirm)."""
        if self.read_only:
            return ""
        parts = [f'<form id="bulk" class="toolbar" method="post" action="{h(self._url(buttons[0][0]))}">',
                 self._hidden(next_path)]
        for action, label, icon_name, danger, confirm in buttons:
            confirm_attr = f' data-confirm="{h(confirm)}"' if confirm else ""
            parts.append(f'<button class="btn sm{" danger" if danger else ""}" formaction="{h(self._url(action))}" '
                         f'data-needs-selection disabled{confirm_attr}>{icon(icon_name)}{h(label)}</button>')
        parts.append('<span class="selected" data-selected-count></span></form>')
        return "".join(parts)

    def _check(self, item_id: str) -> str:
        if self.read_only:
            return ""
        return (f'<td class="check"><input type="checkbox" name="id" value="{h(item_id)}" form="bulk" '
                f'data-row-check aria-label="Select {h(item_id)}"></td>')

    def _check_all(self) -> str:
        return "" if self.read_only else '<th class="check"><input type="checkbox" data-check-all aria-label="Select all"></th>'

    # ---------------------------------------------------------------- layout

    def _page(self, title: str, body: str, *, crumbs: Sequence[Tuple[str, Optional[str]]] = (),
              active: str = "", live: bool = False, status: str = "200 OK",
              stats: Optional[dict] = None) -> Response:
        stats = stats if stats is not None else self.storage.get_statistics()
        notice = ""
        if self._request.notice:
            error = self._request.level == "error"
            notice = (f'<div class="notice{" error" if error else ""}" role="{"alert" if error else "status"}">'
                      f'{icon("failed" if error else "check")}<span>{h(self._request.notice)}</span>'
                      f'<button type="button" data-dismiss aria-label="Dismiss">{icon("x")}</button></div>')

        def nav_link(key: str, label: str, path: str, icon_name: str, count: Optional[int] = None,
                     alert: bool = False) -> str:
            count_html = ""
            if count is not None:
                count_html = f'<span class="count{" alert" if alert and count else ""}">{compact(count)}</span>'
            current = ' aria-current="page"' if key == active else ""
            return (f'<a href="{h(self._url(path))}" class="{"active" if key == active else ""}"{current}>'
                    f'{icon(icon_name)}<span>{h(label)}</span>{count_html}</a>')

        nav = [nav_link("overview", "Overview", "/", "overview"), '<div class="nav-label">Jobs</div>']
        for state in ALL_STATES:
            slug = STATE_META[state][0]
            nav.append(nav_link(slug, state, f"/jobs/{slug}", slug, stats.get(slug, 0), alert=state == FAILED))
        system = [nav_link(key, label, "/" + key, key, stats.get(key, 0))
                  for key, label in (("recurring", "Recurring jobs"), ("servers", "Servers"), ("queues", "Queues"))
                  if self._enabled(key)]
        if system:
            nav += ['<div class="nav-label">System</div>', *system]
        pages = [p for p in self.router.pages.values() if p.nav]
        if pages:
            nav.append('<div class="nav-label">Pages</div>')
            nav += [nav_link("page:" + p.path, p.title, p.path, p.icon if p.icon in assets._ICON_PATHS else "overview")
                    for p in pages]

        crumb_html = [f'<a href="{h(self._url("/"))}">{h(self.title)}</a>']
        for label, url in crumbs:
            crumb_html.append('<span class="sep">/</span>')
            crumb_html.append(f'<a href="{h(self._url(url))}">{h(label)}</a>' if url
                              else f'<span class="current">{h(label)}</span>')

        live_btn = ""
        if live:
            live_btn = ('<button type="button" id="live-btn" class="icon-btn" aria-pressed="true" '
                        'title="Refresh automatically every 5 seconds"><span class="live-dot"></span>'
                        '<span class="live-label">Live</span></button>')
        theme_btn = ('<button type="button" id="theme-btn" class="icon-btn" aria-label="Theme">'
                     + "".join(icon(n, data_icon=n) for n in ("monitor", "sun", "moon")) + "</button>")
        search = ""
        if self._enabled("search"):
            search = (f'<form class="search" role="search" method="get" action="{h(self._url("/search"))}">'
                      f'{icon("search")}<input name="q" placeholder="Go to job ID…" aria-label="Go to job by ID" '
                      f'inputmode="numeric" autocomplete="off"></form>')
        account = ""
        user = self._request.user
        if user is not None:
            account = f'<span class="account" title="{h(user.email or user.id)}">{icon("user")}<span>{h(user.display)}</span></span>'
            if self.auth.supports_logout:
                account += (f'<form method="post" action="{h(self._url("/auth/logout"))}">'
                            f'<input type="hidden" name="csrf" value="{h(self._request.csrf)}">'
                            f'<button class="icon-btn" title="Sign out">{icon("logout")}<span class="live-label">Sign out</span>'
                            f'</button></form>')

        doc = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{h(title)} · {h(self.title)}</title>
<script>{assets.THEME_BOOT}</script>
<link rel="stylesheet" href="{h(self._url('/assets/app.css', v=assets.CSS_VERSION))}">
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='8' fill='%23e8590c'/%3E%3Cpath d='M12.5 18.5A2.5 2.5 0 0 0 15 16c0-1.4-.5-2-1-3-1-2.1-.2-4 2-6 .5 2.5 2 4.9 4 6.5 2 1.6 3 3.5 3 5.5a7 7 0 1 1-14 0c0-1.2.4-2.3 1-3a2.5 2.5 0 0 0 2.5 2.5z' fill='none' stroke='white' stroke-width='2' stroke-linejoin='round'/%3E%3C/svg%3E">
</head><body data-idle-timeout="{int(getattr(self.auth, 'idle_timeout', None) or 0)}" \
data-refresh-interval="{int(max(1.0, self.stats_poll_interval) * 1000)}">
<div class="app">
  <aside class="sidebar">
    <a class="brand" href="{h(self._url('/'))}"><span class="brand-mark">{icon('flame')}</span>{h(self.title)}</a>
    <nav class="nav" id="nav" aria-label="Dashboard">{''.join(nav)}</nav>
    <div class="sidebar-foot">scheduleFire {h(__version__)} · {h(type(self.storage).__name__.replace('Storage', ''))}{' · read-only' if self.read_only else ''}</div>
  </aside>
  <div class="main">
    <header class="topbar">
      <div class="crumbs">{''.join(crumb_html)}</div>
      <div class="top-actions">{search}{live_btn}{theme_btn}{account}</div>
    </header>
    <main class="content" id="content">{notice}{body}</main>
  </div>
</div>
<script src="{h(self._url('/assets/app.js', v=assets.JS_VERSION))}" defer></script>
</body></html>"""
        return status, self._csrf_headers(), doc.encode("utf-8")

    def _head(self, title: str, subtitle: str = "", actions: str = "", extra: str = "") -> str:
        return (f'<div class="page-head"><div style="min-width:0"><h1>{title}</h1>'
                f'{f"<p>{subtitle}</p>" if subtitle else ""}{extra}</div>'
                f'{f"<div class=actions>{actions}</div>" if actions else ""}</div>')

    # -------------------------------------------------------------- overview

    def _overview(self, query: dict) -> Response:
        storage = self.storage
        stats = storage.get_statistics()
        range_ = "7d" if (query.get("range") or [""])[0] == "7d" else "24h"

        def tile(label: str, path: str, icon_name: str, value: int, hint: str, alert: bool = False) -> str:
            return (f'<a class="tile{" alert" if alert else ""}" href="{h(self._url(path))}">'
                    f'<div class="label">{icon(icon_name)}{h(label)}</div>'
                    f'<div class="value">{compact(value)}</div><div class="hint">{h(hint)}</div></a>')

        failed = stats["failed"]
        tiles = "".join([
            tile("Enqueued", "/jobs/enqueued", "enqueued", stats["enqueued"],
                 f"across {_plural(stats['queues'], 'queue')}"),
            tile("Processing", "/jobs/processing", "processing", stats["processing"],
                 f"{_plural(stats['servers'], 'server')} online"),
            tile("Scheduled", "/jobs/scheduled", "scheduled", stats["scheduled"], "delayed jobs and retries"),
            tile("Failed", "/jobs/failed", "failed", failed,
                 "need attention" if failed else "no failures", alert=failed > 0),
            tile("Succeeded", "/jobs/succeeded", "succeeded", stats["succeeded"], "all time"),
            tile("Recurring", "/recurring", "recurring", stats["recurring"], "cron schedules")
            if self._enabled("recurring") else "",
        ])

        if range_ == "24h":
            points = []
            for p in storage.get_hourly_history(24):
                start = datetime.strptime(p["hour"], "%Y-%m-%dT%H")
                points.append({"tick": start.strftime("%H:00"),
                               "label": start.strftime("%b %d, %H:00") + f"–{(start.hour + 1) % 24:02d}:00 UTC",
                               "s": p["succeeded"], "f": p["failed"]})
            chart_sub = "Finished jobs per hour, last 24 hours (UTC)"
        else:
            points = []
            for p in storage.get_daily_history(7):
                day = datetime.strptime(p["date"], "%Y-%m-%d")
                points.append({"tick": day.strftime("%a %d"), "label": day.strftime("%A, %b %d"),
                               "s": p["succeeded"], "f": p["failed"]})
            chart_sub = "Finished jobs per day, last 7 days (UTC)"
        seg = "".join(
            f'<a href="{h(self._url("/", range=key))}" class="{"on" if key == range_ else ""}">{label}</a>'
            for key, label in (("24h", "24 hours"), ("7d", "7 days"))
        )
        chart = (f'<section class="card" style="margin-bottom:16px"><div class="card-head"><div><h2>Job throughput</h2>'
                 f'<div class="sub">{chart_sub}</div></div><div class="right"><nav class="segmented" '
                 f'aria-label="Time range">{seg}</nav></div></div>'
                 f'<div class="card-body">{throughput_chart(points, "Job throughput: " + chart_sub)}</div>'
                 f'{chart_table(points)}</section>')

        failures = storage.get_jobs(FAILED, 0, 5)
        if failures:
            items = []
            for r in failures:
                d = r.state_data
                exc = (d.get("exception_type") or "").rsplit(".", 1)[-1]
                items.append(
                    f'<li><a class="jid mono" href="{h(self._url("/job/" + r.id))}">#{h(r.id)}</a>'
                    f'<div class="grow"><a class="job" href="{h(self._url("/job/" + r.id))}">'
                    f'<span class="fn">{h(r.job.qualname)}</span></a>'
                    f'<div class="meta clip"><span class="error-text">{h(exc)}</span> {h(d.get("exception_message") or "")}</div></div>'
                    f'<div class="meta">{when(parse_iso(d.get("failed_at")))}</div></li>'
                )
            failures_html = f'<ul class="list">{"".join(items)}</ul>'
        else:
            failures_html = empty_state("succeeded", "No failed jobs", "Jobs that exhaust their retries show up here.")

        servers = storage.get_servers()
        if servers:
            now = time.time()
            items = []
            for s in servers:
                items.append(
                    f'<li>{self._health_dot(now - s.heartbeat)}<div class="grow"><div class="clip" style="font-weight:550">'
                    f'{h(s.name)}</div><div class="meta">{_plural(s.worker_count, "worker")} · '
                    f'{h(", ".join(s.queues))}</div></div><div class="meta">{when(s.heartbeat)}</div></li>'
                )
            servers_html = f'<ul class="list">{"".join(items)}</ul>'
        else:
            servers_html = empty_state("servers", "No servers running",
                                       "Start one with <code>python -m schedulefire server</code>.")

        lists = (
            '<div class="grid-2">'
            f'<section class="card"><div class="card-head"><h2>Recent failures</h2><div class="right">'
            f'<a class="card-link" href="{h(self._url("/jobs/failed"))}">View all</a></div></div>{failures_html}</section>'
            f'<section class="card"><div class="card-head"><h2>Servers</h2><div class="right">'
            + (f'<a class="card-link" href="{h(self._url("/servers"))}">Details</a>' if self._enabled("servers") else "")
            + f'</div></div>{servers_html}</section>'
            '</div>'
        )
        body = self._head("Overview", "Real-time status of background jobs across all servers.") + \
            f'<div class="tiles">{tiles}</div>{chart}{lists}'
        return self._page("Overview", body, crumbs=[("Overview", None)], active="overview", live=True, stats=stats)

    @staticmethod
    def _health_dot(age: float) -> str:
        color = "var(--good)" if age < 90 else "var(--warning)"
        return f'<span class="dot" style="background:{color}" title="Last heartbeat {int(age)} s ago"></span>'

    # -------------------------------------------------------------- job list

    def _state_cells(self, state: str, r: JobRecord) -> Tuple[List[str], str]:
        """Extra cells for a job row, and the row's actions."""
        d = r.state_data
        next_path = f"/jobs/{state.lower()}"
        if state == ENQUEUED:
            return [f'<span class="tag mono">{h(d.get("queue") or r.queue)}</span>',
                    when(parse_iso(d.get("enqueued_at")))], ""
        if state == SCHEDULED:
            reason = r.state_reason or ""
            retry_count = r.parameters.get("RetryCount")
            reason_cell = (f'<span class="tag warn">retry {h(str(retry_count))}</span> ' if retry_count else "") \
                + f'<span class="secondary">{h(reason)}</span>'
            return [when(parse_iso(d.get("enqueue_at"))), reason_cell,
                    ], self._row_action("/actions/jobs/requeue", r.id, next_path, "Enqueue now", "play")
        if state == PROCESSING:
            return [f'<span class="mono secondary">{h(d.get("worker_id") or "")}</span>',
                    when(parse_iso(d.get("started_at")))], ""
        if state == SUCCEEDED:
            return [f'<span class="num">{duration(d.get("duration_ms"))}</span>',
                    f'<span class="num secondary">{duration(d.get("latency_ms"))}</span>',
                    when(parse_iso(d.get("succeeded_at")))], ""
        if state == FAILED:
            exc = (d.get("exception_type") or "").rsplit(".", 1)[-1]
            return [f'<div class="clip"><span class="error-text">{h(exc)}</span> '
                    f'<span class="secondary">{h(d.get("exception_message") or "")}</span></div>',
                    when(parse_iso(d.get("failed_at")))], \
                self._row_action("/actions/jobs/requeue", r.id, next_path, "Retry", "requeue")
        if state == DELETED:
            return [f'<span class="clip secondary">{h(r.state_reason or "")}</span>',
                    when(parse_iso(d.get("deleted_at")))], ""
        if state == AWAITING:
            parent = str(d.get("parent_id", ""))
            condition = "Any finished state" if d.get("options") == "OnAnyFinishedState" else "Parent succeeded"
            return [f'<a class="jid mono" href="{h(self._url("/job/" + parent))}">#{h(parent)}</a>',
                    f'<span class="secondary">{condition}</span>', when(r.created_at)], ""
        return [when(r.created_at)], ""

    _STATE_COLUMNS = {
        ENQUEUED: ["Queue", "Enqueued"],
        SCHEDULED: ["Runs", "Reason"],
        PROCESSING: ["Worker", "Started"],
        SUCCEEDED: ['<span class="r">Duration</span>', "Latency", "Completed"],
        FAILED: ["Exception", "Failed"],
        DELETED: ["Reason", "Deleted"],
        AWAITING: ["Parent", "Runs when", "Created"],
    }

    def _job_list(self, state: str, query: dict) -> Response:
        slug, description = STATE_META[state]
        try:
            per_page = int((query.get("per_page") or [self.page_size])[0])
        except ValueError:
            per_page = self.page_size
        if per_page not in _PAGE_SIZES:
            per_page = self.page_size
        try:
            page = max(1, int((query.get("page") or [1])[0]))
        except ValueError:
            page = 1

        stats = self.storage.get_statistics()
        total = self.storage.count_jobs(state)
        pages = max(1, (total + per_page - 1) // per_page)
        page = min(page, pages)
        records = self.storage.get_jobs(state, (page - 1) * per_page, per_page)
        head = self._head(f"{state_dot(state)}{h(state)} jobs", h(description))

        if not records:
            body = head + f'<section class="card">{empty_state(slug, f"No {state.lower()} jobs")}</section>'
            return self._page(f"{state} jobs", body, crumbs=[("Jobs", None), (state, None)], active=slug,
                              live=True, stats=stats)

        list_path = f"/jobs/{slug}" + (f"?page={page}" if page > 1 else "")
        rows = []
        has_actions = False
        for r in records:
            cells, action = self._state_cells(state, r)
            has_actions = has_actions or bool(action)
            url = self._url("/job/" + r.id)
            rows.append(
                f'<tr>{self._check(r.id)}<td class="nowrap"><a class="jid mono" href="{h(url)}">#{h(r.id)}</a></td>'
                f'<td>{job_cell(url, r.job, r.id)}</td>' + "".join(f"<td>{c}</td>" for c in cells)
                + (f'<td class="r">{action}</td>' if has_actions or action else "") + "</tr>"
            )
        columns = "".join(f"<th>{c}</th>" for c in self._STATE_COLUMNS[state])
        table = (f'<div class="table-wrap"><table class="t"><thead><tr>{self._check_all()}<th>ID</th><th>Job</th>'
                 f'{columns}{"<th></th>" if has_actions else ""}</tr></thead><tbody>{"".join(rows)}</tbody></table></div>')
        toolbar = self._bulk_toolbar(list_path, [
            ("/actions/jobs/requeue", "Requeue", "requeue", False, None),
            ("/actions/jobs/delete", "Delete", "deleted", True, "Delete the selected jobs?"),
        ])

        first = (page - 1) * per_page + 1
        last = first + len(records) - 1

        def page_link(p: int, label: str, icon_name: str, enabled: bool) -> str:
            href = self._url(f"/jobs/{slug}", page=p, per_page=per_page)
            inner = f'{icon(icon_name)}<span class="sr-only">{label}</span>'
            if not enabled:
                return f'<span class="btn sm" aria-disabled="true" style="opacity:.4">{inner}</span>'
            return f'<a class="btn sm" href="{h(href)}" aria-label="{label}">{inner}</a>'

        options = "".join(
            f'<option value="{h(self._url(f"/jobs/{slug}", per_page=n))}"{" selected" if n == per_page else ""}>{n} per page</option>'
            for n in _PAGE_SIZES
        )
        pager = (f'<div class="pager"><span>Showing <b class="num">{first:,}–{last:,}</b> of '
                 f'<b class="num">{total:,}</b></span><div class="right">'
                 f'<select data-nav aria-label="Rows per page">{options}</select>'
                 f'{page_link(page - 1, "Previous page", "left", page > 1)}'
                 f'<span class="num">{page} / {pages}</span>'
                 f'{page_link(page + 1, "Next page", "right", page < pages)}</div></div>')
        body = head + f'<section class="card">{toolbar}{table}{pager}</section>'
        return self._page(f"{state} jobs", body, crumbs=[("Jobs", None), (state, None)], active=slug,
                          live=True, stats=stats)

    # ------------------------------------------------------------ job details

    def _job_details(self, job_id: str) -> Response:
        storage = self.storage
        record = storage.get_job(job_id)
        if record is None:
            body = empty_state("search", f"Job #{job_id} not found",
                               "It may have expired: succeeded and deleted jobs are removed automatically.")
            return self._page("Not found", f'<section class="card">{body}</section>',
                              crumbs=[("Jobs", None), (f"#{job_id}", None)], status="404 Not Found")
        job = record.job
        state = record.state_name
        slug = STATE_META.get(state or "", ("",))[0]
        next_path = f"/job/{record.id}"

        actions = ""
        if not self.read_only:
            requeue_label = "Retry" if state == FAILED else "Requeue"
            actions = (self._row_action("/actions/jobs/requeue", record.id, next_path, requeue_label, "requeue")
                       + self._row_action("/actions/jobs/delete", record.id, next_path, "Delete", "deleted",
                                          danger=True, confirm=f"Delete job #{record.id}?"))
            actions = actions.replace('class="btn sm"', 'class="btn"').replace('class="btn sm danger"', 'class="btn danger"')
        head = self._head(f"Job #{h(record.id)} {pill(state, large=True)}", "",
                          actions, extra=f'<code class="signature">{h(job.display_name(300))}</code>')

        left: List[str] = []
        d = record.state_data
        if state == FAILED:
            exc_type = d.get("exception_type") or "Exception"
            left.append(
                f'<section class="card exception"><div class="card-head">{icon("failed")}'
                f'<h2 class="type mono">{h(exc_type)}</h2><div class="right sub">{when(parse_iso(d.get("failed_at")))}</div></div>'
                f'<div class="message">{h(d.get("exception_message") or "(no message)")}</div>'
                f'<details class="trace" open><summary>Stack trace</summary>'
                f'<pre class="code">{h(d.get("exception_details") or "")}</pre></details></section>'
            )

        # arguments
        arg_rows = [(f"args[{i}]", v) for i, v in enumerate(job.args)] + [(k, v) for k, v in job.kwargs.items()]
        if arg_rows:
            args_html = '<dl class="props">' + "".join(
                f'<dt class="mono">{h(k)}</dt><dd><code>{pretty_json(v)}</code></dd>' for k, v in arg_rows) + "</dl>"
        else:
            args_html = '<div class="card-body muted">This job takes no arguments.</div>'
        left.append(f'<section class="card"><div class="card-head"><h2>Arguments</h2>'
                    f'<div class="sub mono">{h(job.method_name)}</div></div>{args_html}</section>')

        if state == SUCCEEDED and "result" in d:
            left.append(f'<section class="card"><div class="card-head"><h2>Result</h2></div>'
                        f'<pre class="code">{pretty_json(d["result"])}</pre></section>')

        # history timeline (newest first); time spent = until the next (newer) state
        history = storage.get_state_history(record.id)
        items = []
        for i, s in enumerate(history):
            key = STATE_META.get(s.name, ("deleted",))[0]
            spent = "current state" if i == 0 else f"for {span_text(history[i - 1].created_at - s.created_at)}"
            tags = []
            sd = s.data
            if sd.get("queue"):
                tags.append(f'<span class="tag">queue <b class="mono">&nbsp;{h(sd["queue"])}</b></span>')
            if sd.get("enqueue_at"):
                ts = parse_iso(sd["enqueue_at"])
                tags.append(f'<span class="tag" title="{absolute(ts) if ts else ""}">runs at&nbsp;'
                            f'<b>{h(datetime.fromtimestamp(ts, timezone.utc).strftime("%H:%M:%S UTC") if ts else "")}</b></span>')
            if sd.get("worker_id"):
                tags.append(f'<span class="tag mono">{h(sd["worker_id"])}</span>')
            if sd.get("duration_ms") is not None:
                tags.append(f'<span class="tag">took&nbsp;<b>{duration(sd["duration_ms"])}</b></span>')
            if sd.get("latency_ms") is not None:
                tags.append(f'<span class="tag">latency&nbsp;<b>{duration(sd["latency_ms"])}</b></span>')
            if sd.get("parent_id"):
                tags.append(f'<a class="tag" href="{h(self._url("/job/" + str(sd["parent_id"])))}">parent&nbsp;'
                            f'<b>#{h(str(sd["parent_id"]))}</b></a>')
            exc = ""
            if sd.get("exception_type") and i > 0:
                exc = (f'<div class="reason"><span class="error-text mono">{h(sd["exception_type"])}</span>: '
                       f'{h(sd.get("exception_message") or "")}</div>')
            items.append(
                f'<li><span class="marker" style="background:var(--s-{key})"></span>'
                f'<div class="row"><span class="name">{h(s.name)}</span><span class="muted" style="font-size:12.5px">'
                f'{spent}</span><span class="when">{when(s.created_at)}</span></div>'
                f'{f"<div class=reason>{h(s.reason)}</div>" if s.reason else ""}{exc}'
                f'{f"<div class=kv>{chr(10).join(tags)}</div>" if tags else ""}</li>'
            )
        left.append(f'<section class="card"><div class="card-head"><h2>History</h2>'
                    f'<div class="sub">{_plural(len(history), "state change")}</div></div>'
                    f'<ol class="timeline">{"".join(items)}</ol></section>')

        # properties
        props = [("State", pill(state)), ("Queue", f'<span class="tag mono">{h(record.queue)}</span>'),
                 ("Function", f'<code>{h(job.method_name)}</code>'), ("Created", when(record.created_at))]
        if record.state_reason and state != FAILED:
            props.append(("Reason", h(record.state_reason)))
        retries = record.parameters.get("RetryCount")
        if retries:
            props.append(("Retry attempts", f'<span class="num">{int(retries)}</span>'))
        if state == PROCESSING:
            props.append(("Worker", f'<code>{h(d.get("worker_id") or "")}</code>'))
        if state == SCHEDULED:
            props.append(("Runs", when(parse_iso(d.get("enqueue_at")))))
        if state == AWAITING:
            parent = str(d.get("parent_id", ""))
            props.append(("Parent", f'<a class="card-link" href="{h(self._url("/job/" + parent))}">#{h(parent)}</a>'))
        props.append(("Expires", when(record.expire_at) if record.expire_at else '<span class="muted">never</span>'))
        continuations = storage.get_continuations(record.id)
        if continuations:
            props.append(("Continuations", " ".join(
                f'<a class="card-link" href="{h(self._url("/job/" + c))}">#{h(c)}</a>' for c in continuations)))
        other_params = {k: v for k, v in record.parameters.items() if k != "RetryCount"}
        right = (f'<section class="card"><div class="card-head"><h2>Details</h2></div><dl class="props">'
                 + "".join(f"<dt>{k}</dt><dd>{v}</dd>" for k, v in props) + "</dl></section>")
        if state == SCHEDULED and not self.read_only:
            default_at = (datetime.now(timezone.utc) + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M")
            right += (
                f'<section class="card"><div class="card-head"><h2>Reschedule</h2></div>'
                f'<form method="post" action="{h(self._url("/actions/jobs/reschedule"))}" class="edit-form" '
                f'style="padding:16px">{self._hidden(next_path)}<input type="hidden" name="id" value="{h(record.id)}">'
                f'<div class="reschedule-mode">'
                f'<label><input type="radio" name="when_mode" value="in" data-freq-select checked>In</label>'
                f'<label><input type="radio" name="when_mode" value="at" data-freq-select>At a specific time</label>'
                f'</div>'
                f'<div class="freq-group show" data-freq="in">'
                f'<label class="field"><span>Run in</span><input type="number" name="amount" value="1" min="0" '
                f'step="any" style="width:80px"></label>'
                f'<label class="field"><span>&nbsp;</span><select name="unit">'
                f'<option value="minutes">minutes</option><option value="hours">hours</option>'
                f'<option value="days">days</option></select></label></div>'
                f'<div class="freq-group" data-freq="at">'
                f'<label class="field"><span>Run at (UTC)</span>'
                f'<input type="datetime-local" name="at" value="{h(default_at)}"></label></div>'
                f'<div class="edit-actions"><button class="btn sm">{icon("scheduled")}Reschedule</button></div>'
                f'</form></section>'
            )
        if other_params:
            right += (f'<section class="card"><div class="card-head"><h2>Parameters</h2></div>'
                      f'<pre class="code">{pretty_json(other_params)}</pre></section>')

        body = head + f'<div class="grid-main"><div class="stack">{"".join(left)}</div><div class="stack">{right}</div></div>'
        crumbs = [("Jobs", None)]
        if state:
            crumbs.append((state, f"/jobs/{slug}"))
        crumbs.append((f"#{record.id}", None))
        return self._page(f"Job #{record.id}", body, crumbs=crumbs, active=slug)

    def _custom_page(self, page: CustomPage, query: dict) -> Response:
        context = PageContext(storage=self.storage, query=query, user=self._request.user,
                              url=lambda path, **params: self._url(path, **params))
        body = self._head(h(page.title)) + page.render(context)
        return self._page(page.title, body, crumbs=[(page.title, None)], active="page:" + page.path)

    def _search(self, query: dict) -> Response:
        q = (query.get("q") or [""])[0].strip().lstrip("#")
        if q.isdigit() and self.storage.get_job(q) is not None:
            return "303 See Other", [("Location", self._url(f"/job/{q}"))], b""
        text = f"There is no job with ID “{h(q)}”. Finished jobs are removed after they expire." if q else \
            "Enter a job ID to jump to it."
        return self._page("Search", f'<section class="card">{empty_state("search", "No matching job", text)}</section>',
                          crumbs=[("Search", None)], status="404 Not Found" if q else "200 OK")

    # ------------------------------------------------------------- recurring

    def _schedule_builder_form(self, r: RecurringJobRecord) -> str:
        """A friendly editor for a recurring job's schedule: a frequency picker that swaps in
        plain fields (a time, a day, an interval) for the common cases, with the raw cron
        expression as a "Custom" fallback for anything else. Pre-filled from ``r.cron`` when
        it's recognized as one of those common cases."""
        state = schedule_builder_state(r.cron)
        freq = state["frequency"]

        def opt(value: str, label: str) -> str:
            return f'<option value="{value}"{" selected" if value == freq else ""}>{label}</option>'

        freq_select = (
            '<select name="frequency" data-freq-select>'
            + opt("minutes", "Every few minutes") + opt("hourly", "Hourly") + opt("daily", "Daily")
            + opt("weekly", "Weekly") + opt("monthly", "Monthly") + opt("custom", "Custom (cron expression)")
            + "</select>"
        )

        def group(name: str, inner: str) -> str:
            return f'<div class="freq-group{" show" if freq == name else ""}" data-freq="{name}">{inner}</div>'

        groups = group("minutes",
            f'<label class="field"><span>Every</span><div class="inline-unit">'
            f'<input type="number" name="interval_minutes" min="1" max="59" '
            f'value="{h(state.get("interval_minutes", "15"))}"><span>minutes</span></div></label>')
        groups += group("hourly",
            f'<label class="field"><span>Every</span><div class="inline-unit">'
            f'<input type="number" name="interval_hours" min="1" max="23" value="{h(state.get("interval_hours", "1"))}">'
            f'<span>hour(s)</span></div></label>'
            f'<label class="field"><span>At minute</span>'
            f'<input type="number" name="minute" min="0" max="59" value="{h(state.get("minute", "0"))}"></label>')
        default_time = state.get("time", "09:00")
        groups += group("daily",
            f'<label class="field"><span>At</span><input type="time" name="time_daily" value="{h(default_time)}"></label>')
        day_options = "".join(
            f'<option value="{i}"{" selected" if str(i) == state.get("day_of_week") else ""}>{name}</option>'
            for i, name in enumerate(DAYS_OF_WEEK)
        )
        groups += group("weekly",
            f'<label class="field"><span>On</span><select name="day_of_week">{day_options}</select></label>'
            f'<label class="field"><span>At</span><input type="time" name="time_weekly" value="{h(default_time)}"></label>')
        groups += group("monthly",
            f'<label class="field"><span>On day</span>'
            f'<input type="number" name="day_of_month" min="1" max="31" value="{h(state.get("day_of_month", "1"))}"></label>'
            f'<label class="field"><span>At</span><input type="time" name="time_monthly" value="{h(default_time)}"></label>')
        groups += group("custom",
            f'<label class="field grow"><span>Cron expression</span>'
            f'<input name="cron" value="{h(state.get("cron", r.cron))}" spellcheck="false"></label>')

        tz_id, q_id = f"tz-{h(r.id)}", f"q-{h(r.id)}"
        tz_options = "".join(f'<option value="{h(z)}">' for z in COMMON_TIME_ZONES)
        queue_names = sorted({q.name for q in self.storage.get_queues()} | {r.queue, "default"})
        q_options = "".join(f'<option value="{h(q)}">' for q in queue_names)

        return (
            f'<form method="post" action="{h(self._url("/actions/recurring/update"))}" class="edit-form">'
            f'{self._hidden("/recurring")}<input type="hidden" name="id" value="{h(r.id)}">'
            f'<label class="field"><span>Frequency</span>{freq_select}</label>'
            f'{groups}'
            f'<label class="field"><span>Time zone</span>'
            f'<input name="time_zone" list="{tz_id}" value="{h(r.time_zone)}" required spellcheck="false"></label>'
            f'<datalist id="{tz_id}">{tz_options}</datalist>'
            f'<label class="field"><span>Queue</span>'
            f'<input name="queue" list="{q_id}" value="{h(r.queue)}" required spellcheck="false"></label>'
            f'<datalist id="{q_id}">{q_options}</datalist>'
            f'<div class="edit-actions"><button class="btn primary sm">Save</button>'
            f'<span class="muted">the job itself (function and arguments) stays unchanged</span></div>'
            f'</form>'
        )

    def _recurring(self) -> Response:
        records = self.storage.get_recurring_jobs()
        head = self._head("Recurring jobs", "Jobs enqueued automatically on a cron schedule. "
                                            "Hover a time to see it in UTC.")
        if not records:
            body = head + '<section class="card">' + empty_state(
                "recurring", "No recurring jobs",
                "Add one with <code>RecurringJob.add_or_update(\"id\", func, Cron.daily())</code>.") + "</section>"
            return self._page("Recurring jobs", body, crumbs=[("Recurring jobs", None)], active="recurring", live=True)

        rows = []
        for r in records:
            description = describe_cron(r.cron)
            schedule = (f'<div style="font-weight:550">{h(description)}</div>' if description else "") + \
                f'<div class="muted"><span class="mono">{h(r.cron)}</span>' + \
                (f' · {h(r.time_zone)}' if r.time_zone != "UTC" or description else "") + "</div>"
            if r.last_job_id:
                last_job = self.storage.get_job(r.last_job_id)
                last = (f'<div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">'
                        f'<a class="jid mono" href="{h(self._url("/job/" + r.last_job_id))}">#{h(r.last_job_id)}</a>'
                        f'{pill(last_job.state_name) if last_job else "<span class=muted>expired</span>"}</div>'
                        f'<div class="muted" style="font-size:12.5px">{when(r.last_execution)}</div>')
            else:
                last = '<span class="muted">never run</span>'
            error = f'<div class="error-text" style="font-size:12.5px">{h(r.error)}</div>' if r.error else ""
            job = r.job
            edit_button = "" if self.read_only else (
                f'<button class="btn sm" type="button" data-open-edit>{icon("edit")}Edit</button>')
            rows.append(
                f'<tr>{self._check(r.id)}<td><div style="font-weight:600">{h(r.id)}</div>{error}</td>'
                f'<td>{schedule}</td>'
                f'<td><div class="job"><span class="fn">{h(job.qualname)}</span>'
                f'<span class="args mono">{h(job.display_name(120)[len(job.qualname):])}</span></div>'
                f'<div class="muted" style="font-size:12.5px">queue <span class="mono">{h(r.queue)}</span></div></td>'
                f'<td>{when(r.next_execution, "never") if r.next_execution else "<span class=muted>never</span>"}</td>'
                f'<td>{last}</td>'
                f'<td class="r" style="display:flex;gap:6px;justify-content:flex-end">'
                f'{self._row_action("/actions/recurring/trigger", r.id, "/recurring", "Trigger", "play")}'
                f'{edit_button}</td></tr>'
            )
            if not self.read_only:
                rows.append(
                    f'<tr><td colspan="7" style="padding:0">'
                    f'<details class="edit"><summary class="sr-only">Edit {h(r.id)}</summary>'
                    f'{self._schedule_builder_form(r)}'
                    f'</details></td></tr>'
                )
        table = (f'<div class="table-wrap"><table class="t"><thead><tr>{self._check_all()}<th>ID</th><th>Schedule</th>'
                 f'<th>Job</th><th>Next run</th><th>Last run</th><th></th></tr></thead>'
                 f'<tbody>{"".join(rows)}</tbody></table></div>')
        toolbar = self._bulk_toolbar("/recurring", [
            ("/actions/recurring/trigger", "Trigger now", "play", False, None),
            ("/actions/recurring/delete", "Delete", "deleted", True, "Delete the selected recurring jobs?"),
        ])
        body = head + f'<section class="card">{toolbar}{table}</section>'
        return self._page("Recurring jobs", body, crumbs=[("Recurring jobs", None)], active="recurring", live=True)

    # --------------------------------------------------------------- servers

    def _servers(self) -> Response:
        servers = self.storage.get_servers()
        head = self._head("Servers", "Processes running workers against this storage. A server that stops "
                                     "sending heartbeats is removed automatically.")
        if not servers:
            body = head + '<section class="card">' + empty_state(
                "servers", "No servers running",
                "Start one with <code>python -m schedulefire server</code> or <code>BackgroundJobServer().run()</code>.") + \
                "</section>"
            return self._page("Servers", body, crumbs=[("Servers", None)], active="servers", live=True)
        now = time.time()
        cards = []
        for s in servers:
            age = now - s.heartbeat
            health = (f'<span class="health">{self._health_dot(age)}{"Active" if age < 90 else "Not responding"}</span>')
            queues = " ".join(f'<span class="tag mono">{h(q)}</span>' for q in s.queues)
            requested = s.requested or {}
            pending_workers = pending_queues = ""
            if "worker_count" in requested and requested["worker_count"] != s.worker_count:
                pending_workers = (f' <span class="pending" title="Waiting for the server to apply it">'
                                   f'→ {int(requested["worker_count"])} pending</span>')
            if "queues" in requested and requested["queues"] != s.queues:
                pending_queues = (f' <span class="pending" title="Waiting for the server to apply it">→ '
                                  f'{h(", ".join(requested["queues"]))} pending</span>')
            edit = ""
            if not self.read_only:
                edit = (
                    f'<details class="edit"><summary class="btn sm">{icon("edit")}Edit</summary>'
                    f'<form method="post" action="{h(self._url("/actions/servers/update"))}" class="edit-form">'
                    f'{self._hidden("/servers")}<input type="hidden" name="id" value="{h(s.id)}">'
                    f'<label class="field"><span>Workers</span><input type="number" name="workers" min="1" '
                    f'max="{MAX_WORKERS}" required value="{int(requested.get("worker_count", s.worker_count))}"></label>'
                    f'<label class="field grow"><span>Queues <em>highest priority first, comma-separated</em></span>'
                    f'<input name="queues" required pattern="[a-z0-9_\\-]+(\\s*,\\s*[a-z0-9_\\-]+)*" '
                    f'value="{h(", ".join(requested.get("queues", s.queues)))}" spellcheck="false" autocomplete="off"></label>'
                    f'<div class="edit-actions"><button class="btn primary sm">Apply</button>'
                    f'<span class="muted">Applied live, no restart. Workers that are removed finish their current job first.</span></div>'
                    f'</form></details>'
                )
            cards.append(
                f'<section class="card"><div class="card-head"><div style="min-width:0"><h2 class="clip">{h(s.name)}</h2>'
                f'<div class="sub mono clip">{h(s.id)}</div></div><div class="right">{health}</div></div>'
                f'<dl class="props"><dt>Workers</dt><dd><span class="num">{s.worker_count}</span>{pending_workers}</dd>'
                f'<dt>Queues</dt><dd>{queues}{pending_queues}</dd><dt>Started</dt><dd>{when(s.started_at)}</dd>'
                f'<dt>Heartbeat</dt><dd>{when(s.heartbeat)}</dd></dl>{edit}</section>'
            )
        body = head + f'<div class="servers">{"".join(cards)}</div>'
        return self._page("Servers", body, crumbs=[("Servers", None)], active="servers", live=True)

    # ---------------------------------------------------------------- queues

    def _queues(self) -> Response:
        queues = self.storage.get_queues()
        head = self._head("Queues", "Jobs waiting in each queue, and jobs fetched by a worker.")
        if not queues:
            body = head + '<section class="card">' + empty_state("queues", "All queues are empty",
                                                                 "Enqueued jobs appear here until a worker picks them up.") + "</section>"
            return self._page("Queues", body, crumbs=[("Queues", None)], active="queues", live=True)
        rows = "".join(
            f'<tr><td><span class="tag mono">{h(q.name)}</span></td><td class="r num">{q.enqueued:,}</td>'
            f'<td class="r num">{q.fetched:,}</td></tr>' for q in queues
        )
        body = head + (f'<section class="card"><div class="table-wrap"><table class="t"><thead><tr><th>Queue</th>'
                       f'<th class="r">Waiting</th><th class="r">Fetched</th></tr></thead><tbody>{rows}</tbody></table>'
                       f'</div><div class="pager"><a class="card-link" href="{h(self._url("/jobs/enqueued"))}">'
                       f'View enqueued jobs</a></div></section>')
        return self._page("Queues", body, crumbs=[("Queues", None)], active="queues", live=True)

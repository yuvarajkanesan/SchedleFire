"""Standalone serving and mounting helpers for the dashboard."""

from __future__ import annotations

import logging
import threading
from socketserver import ThreadingMixIn
from typing import Callable, Optional
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

from ..storage.base import JobStorage
from .app import DashboardApp
from .routing import DEFAULT_PATH

logger = logging.getLogger("schedulefire.dashboard")


def mount_dashboard(wsgi_app: Callable, prefix: str = DEFAULT_PATH, storage: Optional[JobStorage] = None, *,
                    dashboard: Optional[DashboardApp] = None, **options) -> Callable:
    """Wrap any WSGI app (Flask, Django, Bottle, Pyramid...) so ``prefix`` serves the dashboard.

    The default prefix, ``/schedule-fire``, keeps the dashboard clear of your own routes.
    Everything else is passed to your app untouched.

    ::

        app.wsgi_app = mount_dashboard(app.wsgi_app)                    # Flask -> /schedule-fire
        application = mount_dashboard(get_wsgi_application())           # Django wsgi.py
        app.wsgi_app = mount_dashboard(app.wsgi_app, "/admin/jobs", auth=OAuth(...), routes={...})
    """
    dashboard = dashboard or DashboardApp(storage, **options)
    prefix = "/" + prefix.strip("/")
    if prefix == "/":
        return dashboard

    def app(environ, start_response):
        path = environ.get("PATH_INFO", "") or "/"
        if path == prefix or path.startswith(prefix + "/"):
            environ = dict(environ)
            environ["SCRIPT_NAME"] = environ.get("SCRIPT_NAME", "").rstrip("/") + prefix
            environ["PATH_INFO"] = path[len(prefix):] or "/"
            return dashboard(environ, start_response)
        return wsgi_app(environ, start_response)

    app.dashboard = dashboard
    return app


class _QuietHandler(WSGIRequestHandler):
    def log_message(self, format, *args):  # noqa: A002 - signature from BaseHTTPRequestHandler
        logger.debug("%s - %s", self.address_string(), format % args)


class _ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True
    app: Optional[DashboardApp] = None

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            if self.app is not None:
                self.app.storage.close()  # the request thread's connection


def _redirect_to(prefix: str) -> Callable:
    def app(environ, start_response):
        if (environ.get("PATH_INFO") or "/") == "/":
            start_response("302 Found", [("Location", prefix + "/"), ("Content-Length", "0")])
            return [b""]
        start_response("404 Not Found", [("Content-Type", "text/plain"), ("Content-Length", "9")])
        return [b"Not Found"]
    return app


def run_dashboard(
    storage: Optional[JobStorage] = None,
    host: str = "127.0.0.1",
    port: int = 8080,
    *,
    path: str = DEFAULT_PATH,
    background: bool = False,
    dashboard: Optional[DashboardApp] = None,
    **options,
) -> WSGIServer:
    """Serve the dashboard under ``path`` (default ``/schedule-fire``; ``"/"`` for the site root;
    requests to ``/`` redirect there). With ``background=True`` returns
    immediately (call ``.shutdown()`` to stop). ``options`` go to :class:`DashboardApp`
    (``auth``, ``routes``, ``read_only``, ...). Pass a pre-built ``dashboard=DashboardApp(...)``
    instead (e.g. one with jobs registered via ``add_job``/``.job()``, or custom pages) to serve
    that app as-is; ``storage`` and ``options`` are then ignored."""
    app = dashboard or DashboardApp(storage, **options)
    prefix = "/" + path.strip("/") if path.strip("/") else "/"
    wsgi = app if prefix == "/" else mount_dashboard(_redirect_to(prefix), prefix, dashboard=app)
    httpd = make_server(host, port, wsgi, server_class=_ThreadingWSGIServer, handler_class=_QuietHandler)
    httpd.app = app
    logger.info("Dashboard listening on http://%s:%d%s (auth: %s)", host, httpd.server_port,
                prefix if prefix != "/" else "/", app.auth.describe())
    if host not in ("127.0.0.1", "localhost", "::1"):
        if app.auth.name == "none":
            logger.warning("Dashboard is reachable from the network on %s WITHOUT authentication: anyone who can "
                           "connect can see and manage jobs. Enable basic or oauth authentication.", host)
        elif app.auth.name != "local":
            logger.warning("Dashboard is reachable from the network over plain HTTP: logins and job data are "
                           "sent unencrypted. Put it behind an HTTPS reverse proxy.")
    if background:
        threading.Thread(target=httpd.serve_forever, name="schedulefire-dashboard", daemon=True).start()
        return httpd
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return httpd

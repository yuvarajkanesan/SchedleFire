"""Web dashboard - monitor and manage jobs, like the Hangfire Dashboard.

It's a plain WSGI app with no external assets, so it runs standalone::

    from schedulefire.dashboard import run_dashboard
    run_dashboard(storage, port=8080)

or inside another framework: ``mount_dashboard(app.wsgi_app)`` for Flask/Django,
``app.mount("/schedule-fire", a2wsgi.WSGIMiddleware(DashboardApp(storage)))`` for FastAPI.
It lives under ``/schedule-fire`` by default so it never collides with your own routes.

By default only local requests are allowed (as in Hangfire). Pass ``basic_auth=("user",
"password")`` or an ``authorization(environ) -> bool`` callable to open it up.
"""

from .app import DashboardApp, PageContext
from .auth import BasicAuth, DashboardAuth, LocalOnlyAuth, NoAuth, OAuth, User, hash_password, verify_password
from .routing import CANONICAL_ROUTES, DEFAULT_PATH
from .server import mount_dashboard, run_dashboard

__all__ = [
    "BasicAuth",
    "CANONICAL_ROUTES",
    "DEFAULT_PATH",
    "DashboardApp",
    "DashboardAuth",
    "LocalOnlyAuth",
    "NoAuth",
    "OAuth",
    "PageContext",
    "User",
    "hash_password",
    "mount_dashboard",
    "run_dashboard",
    "verify_password",
]

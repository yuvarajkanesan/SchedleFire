"""Command line: ``python -m schedulefire server|dashboard|config ...``

Settings come from the config file (``--config``, ``$SCHEDULEFIRE_CONFIG`` or
``./schedulefire.toml``); command-line flags override individual values.
"""

from __future__ import annotations

import argparse
import os
import sys

from . import __version__
from .settings import TEMPLATE, ConfigError, Settings, load_config


def _load(args, parser) -> Settings:
    if args.config and not os.path.exists(args.config):
        parser.error(f"config file not found: {args.config}")
    env = dict(os.environ)
    if getattr(args, "db", None):
        env["SCHEDULEFIRE_DB"] = args.db
    if getattr(args, "storage", None):
        env["SCHEDULEFIRE_STORAGE"] = args.storage
    try:
        return load_config(args.config, environ=env)
    except ConfigError as e:
        parser.exit(2, f"schedulefire: configuration error: {e}\n")


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(prog="schedulefire", description="scheduleFire background job processing")
    parser.add_argument("--version", action="version", version=f"scheduleFire {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", "-c", default=None,
                        help="config file (default: $SCHEDULEFIRE_CONFIG or ./schedulefire.toml / .json)")
    common.add_argument("--db", default=None,
                        help="storage URL or SQLite path; overrides the config file's storage")
    common.add_argument("--storage", default=None, metavar="NAME",
                        help="which [storage.NAME] profile of the config file to use")
    common.add_argument("--log-level", default=None)
    common.add_argument("--path", default=".", help="directory added to sys.path so job modules import")

    server = sub.add_parser("server", parents=[common], help="run a background job server")
    server.add_argument("--queues", default=None, help="comma-separated, highest priority first")
    server.add_argument("--workers", type=int, default=None, help="worker threads (default: min(cpu*5, 20))")
    server.add_argument("--name", default=None, help="server name shown in the dashboard")
    server.add_argument("--import", dest="imports", action="append", default=[],
                        help="module to import at startup (e.g. to register filters); repeatable")
    server.add_argument("--schedule-interval", type=float, default=None,
                        help="seconds between delayed/recurring scheduler runs")
    server.add_argument("--dashboard", type=int, metavar="PORT", default=None,
                        help="also serve the dashboard on this port")

    dash = sub.add_parser("dashboard", parents=[common], help="run the web dashboard")
    dash.add_argument("--host", default=None)
    dash.add_argument("--port", type=int, default=None)
    dash.add_argument("--user", help="enable basic auth with this user (use with SCHEDULEFIRE_DASHBOARD_PASSWORD)")

    hp = sub.add_parser("hash-password", help="hash a dashboard password for [dashboard.users]")
    hp.add_argument("--stdin", action="store_true", help="read the password from standard input")

    cfg = sub.add_parser("config", help="create or check a configuration file")
    cfg_sub = cfg.add_subparsers(dest="action", required=True)
    init = cfg_sub.add_parser("init", help="write a commented schedulefire.toml template")
    init.add_argument("file", nargs="?", default="schedulefire.toml")
    init.add_argument("--force", action="store_true", help="overwrite an existing file")
    check = cfg_sub.add_parser("check", parents=[common], help="validate the config and test the storage connection")
    check.add_argument("--no-connect", action="store_true", help="only validate, don't connect to the storage")

    args = parser.parse_args(argv)

    if args.command == "hash-password":
        from .dashboard.auth import hash_password

        if args.stdin:
            password = sys.stdin.readline().rstrip("\r\n")
        else:
            import getpass

            password = getpass.getpass("Password: ")
            if getpass.getpass("Repeat: ") != password:
                parser.exit(1, "Passwords don't match.\n")
        if len(password) < 8:
            parser.exit(1, "Use at least 8 characters.\n")
        print(hash_password(password))
        return

    if args.command == "config" and args.action == "init":
        if os.path.exists(args.file) and not args.force:
            parser.exit(1, f"{args.file} already exists (use --force to overwrite)\n")
        with open(args.file, "w", encoding="utf-8") as f:
            f.write(TEMPLATE)
        print(f"Wrote {args.file}. Edit it, then run: schedulefire config check")
        return

    sys.path.insert(0, os.path.abspath(args.path))
    settings = _load(args, parser)
    if args.log_level:
        settings.log_level = args.log_level.upper()

    if args.command == "config":  # check
        print(settings.describe())
        if not args.no_connect:
            try:
                storage = settings.create_storage()
                stats = storage.get_statistics()
                storage.dispose()
            except Exception as e:
                parser.exit(1, f"\nstorage connection FAILED: {type(e).__name__}: {e}\n")
            print(f"\nstorage connection OK ({stats['enqueued']} enqueued, {stats['failed']} failed, "
                  f"{stats['servers']} servers)")
        return

    if args.command == "server":
        s = settings.server
        if args.queues:
            s.queues = [q.strip() for q in args.queues.split(",") if q.strip()]
        if args.workers:
            s.workers = args.workers
        if args.name:
            s.name = args.name
        if args.schedule_interval is not None:
            s.schedule_polling_interval = args.schedule_interval
        s.imports = s.imports + args.imports
        if args.dashboard is not None:
            settings.dashboard.enabled, settings.dashboard.port = True, args.dashboard
        try:
            storage = settings.apply(configure_logging=True, register_recurring=True)
        except ConfigError as e:
            parser.exit(2, f"schedulefire: configuration error: {e}\n")
        if settings.dashboard.enabled:
            from .dashboard import run_dashboard

            run_dashboard(storage, host=settings.dashboard.host, port=settings.dashboard.port,
                          path=settings.dashboard.path, background=True, **settings.dashboard.app_kwargs())
        settings.create_server(storage).run()
        return

    # dashboard
    d = settings.dashboard
    if args.host:
        d.host = args.host
    if args.port:
        d.port = args.port
    if args.user:
        password = os.environ.get("SCHEDULEFIRE_DASHBOARD_PASSWORD")
        if not password:
            parser.error("--user requires the SCHEDULEFIRE_DASHBOARD_PASSWORD environment variable")
        d.username, d.password, d.auth = args.user, password, "basic"
    storage = settings.apply(configure_logging=True)
    from .dashboard import run_dashboard

    run_dashboard(storage, host=d.host, port=d.port, path=d.path, **d.app_kwargs())


if __name__ == "__main__":
    main()

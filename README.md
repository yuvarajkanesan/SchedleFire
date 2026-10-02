# scheduleFire

> The package and import name is lowercase, following Python convention: `pip install schedulefire`, `import schedulefire`, `python -m schedulefire`.

Background job processing for Python, modelled on .NET's **Hangfire**. It supports:

- fire-and-forget, delayed, recurring (cron) and continuation jobs
- automatic retries and queues
- a web dashboard
- storage in SQL Server, PostgreSQL, MySQL/MariaDB, Redis or SQLite, or in memory with no database at all

It runs on **Python 3.7 through 3.14**. The full test suite passes on every one of those versions.
On Python 3.11+ the core has **no dependencies** at all. On older versions, pip automatically adds small
backports of newer standard-library modules: `tomli` below 3.11, and `backports.zoneinfo` below 3.9.
Database drivers for the server-based backends are optional installs.

```python
import schedulefire
from datetime import timedelta
from schedulefire import BackgroundJob, RecurringJob, Cron, SQLiteStorage, BackgroundJobServer

schedulefire.configure(storage=SQLiteStorage("jobs.db"))

job_id = BackgroundJob.enqueue(send_email, "ada@example.com", subject="Welcome")   # fire-and-forget
BackgroundJob.schedule(send_email, timedelta(hours=1), "bob@example.com")         # delayed
BackgroundJob.continue_job_with(job_id, log_email_sent, "ada@example.com")         # continuation
RecurringJob.add_or_update("nightly-report", build_report, Cron.daily(2, 30))      # recurring

BackgroundJobServer(queues=["critical", "default"]).run()                          # process jobs
```

## Hangfire → scheduleFire

| Hangfire (C#) | scheduleFire (Python) |
|---|---|
| `GlobalConfiguration.Configuration.UseSqlServerStorage(connStr)` | `schedulefire.configure(storage=SQLServerStorage(connStr))` |
| `BackgroundJob.Enqueue(() => F(x))` | `BackgroundJob.enqueue(f, x)` |
| `BackgroundJob.Enqueue("critical", () => F(x))` | `BackgroundJob.enqueue_to("critical", f, x)` |
| `BackgroundJob.Schedule(() => F(x), TimeSpan.FromHours(1))` | `BackgroundJob.schedule(f, timedelta(hours=1), x)` |
| `BackgroundJob.ContinueJobWith(id, () => F())` | `BackgroundJob.continue_job_with(id, f)` |
| `BackgroundJob.Delete(id)` / `Requeue(id)` | `BackgroundJob.delete(id)` / `requeue(id)` |
| `RecurringJob.AddOrUpdate("id", () => F(), Cron.Daily())` | `RecurringJob.add_or_update("id", f, Cron.daily())` |
| `RecurringJob.TriggerJob("id")` / `RemoveIfExists("id")` | `RecurringJob.trigger("id")` / `remove_if_exists("id")` |
| `new BackgroundJobServer(options)` | `BackgroundJobServer(...).start()` / `.run()` / `with` block |
| `[Queue("critical")]` | `@queue("critical")` |
| `[AutomaticRetry(Attempts = 3)]` | `@AutomaticRetry(attempts=3)` |
| `[DisableConcurrentExecution(600)]` | `@DisableConcurrentExecution(timeout=600)` |
| `GlobalJobFilters.Filters.Add(...)` | `GlobalJobFilters.add(...)` |
| `IJobCancellationToken.ThrowIfCancellationRequested()` | `get_job_context().throw_if_cancellation_requested()` |
| `PerformContext` | `get_job_context()` (job id, retry count, parameters) |
| `JobActivator` (DI) | `JobActivator` subclass → `schedulefire.configure(activator=...)` |
| `app.UseHangfireDashboard()` | `run_dashboard(...)` or mount `DashboardApp` (WSGI) |
| `SetJobParameter` / `GetJobParameter` | `BackgroundJob.set_parameter(id, name, value)` / `get_parameter(id, name)` |
| `IBackgroundProcess` | `BackgroundProcess` subclass → `BackgroundJobServer(background_processes=[...])` |
| `DashboardOptions.StatsPollingInterval` | `DashboardApp(stats_poll_interval=...)` |

Hangfire parses a lambda expression tree. Python can't do that, so scheduleFire takes the
function and its arguments separately: `enqueue(func, *args, **kwargs)`.

## Install / run

```bash
pip install -e .                 # from this folder: core only, SQLite + memory storage
pip install -e ".[postgres]"     # + PostgreSQL (psycopg 3)
pip install -e ".[redis]"        # + Redis (redis-py)
pip install -e ".[mysql]"        # + MySQL / MariaDB (PyMySQL)
pip install -e ".[sqlserver]"    # + SQL Server / Azure SQL (pyodbc + a Microsoft ODBC driver)
pip install -e ".[tz]"           # + time zone data for recurring jobs on Windows

python -m schedulefire server --db jobs.db --queues critical,default --workers 10 --dashboard 8080
python -m schedulefire server --db postgresql://user:pw@db-host/app
python -m schedulefire server --db redis://redis-host:6379/0
python -m schedulefire server --db mysql://user:pw@db-host:3306/app
python -m schedulefire server --db "mssql://user:pw@sql-host:1433/app?TrustServerCertificate=yes"
python -m schedulefire dashboard --db jobs.db --port 8080
```

`--db` accepts a SQLite file path or a `postgresql://`, `mysql://`, `mssql://` or `redis://` URL. You can also set it with
the `SCHEDULEFIRE_DB` environment variable.

To run everything in one process, run `python -m examples.quickstart` and open http://127.0.0.1:8080/schedule-fire/.

## Configuration file

Everything can live in one `schedulefire.toml` (or `schedulefire.json`):

- connection details for every database you use, with one line to choose which is active
- workers, queues and retries
- job expiration
- the dashboard, logging and recurring jobs

```bash
schedulefire config init      # writes a commented template with every option
schedulefire config check     # validates it, shows the result (passwords hidden) and tests the connection
```

```toml
[storage]
use = "postgres"                     # the active one; switch per environment with SCHEDULEFIRE_STORAGE=mysql

[storage.sqlite]
path = "data/schedulefire.db"

[storage.postgres]
host = "${PG_HOST:-localhost}"       # ${VAR} reads the environment; ${VAR:-default} gives a fallback
user = "app"
password = "${PG_PASSWORD}"          # keep secrets in the environment, not in the file
database = "app"

[storage.mysql]
url = "mysql://app:${MYSQL_PASSWORD}@db:3306/app"      # a URL works too

[storage.sqlserver]
host = "sql01"
database = "app"                     # no user -> Windows authentication
trust_server_certificate = true

[storage.redis]
host = "cache"

[jobs]
expiration = "1d"                    # durations: seconds or "500ms", "15s", "5m", "1h30m", "1d"

[retry]
attempts = 5
delays = ["10s", "1m", "5m"]
except_on = ["builtins.ValueError"]

[server]
workers = 20
queues = ["critical", "default"]
imports = ["myapp.jobs"]

[dashboard]
enabled = true                       # `schedulefire server` serves it too
port = 8080
username = "admin"
password = "${DASHBOARD_PASSWORD}"

[logging]
level = "INFO"

[[recurring]]                        # created or updated when the server starts
id = "nightly-report"
job = "myapp.jobs:build_report"
cron = "30 2 * * *"
```

**How it's used.**

- `schedulefire server` and `schedulefire dashboard` read the file automatically. Command-line flags
  (`--workers`, `--queues`, `--port`, `--storage NAME`, `--db URL`, ...) override single values.
- In your app, `BackgroundJob.enqueue(...)` also finds the file, so no `configure()` call is needed.
- To load a file explicitly: `settings = schedulefire.load_config("path.toml")`. Then call
  `settings.apply()`, or `settings.create_storage()` / `settings.create_server()` to build the pieces yourself.

**Where the file is looked for:** `--config PATH`, then `$SCHEDULEFIRE_CONFIG`, then `./schedulefire.toml`,
then `./schedulefire.json`.

**Precedence, highest first:**

1. code: `configure(...)`
2. command-line flags
3. `SCHEDULEFIRE_DB` (a URL) and `SCHEDULEFIRE_STORAGE` (a profile name)
4. the config file
5. defaults

**Mistakes are caught early.** Unknown or misspelled keys fail with the file name and a suggestion, for example
`unknown setting "wokers" - did you mean "workers"?`. So do wrong types, invalid queue names, bad cron
expressions and missing environment variables.

TOML files work on every supported Python version. On 3.7–3.10, pip installs the `tomli` package with scheduleFire.

## Choosing a storage

Every backend supports every feature and runs the same test suite. What differs is where they can run:

| Storage | Use it when | Multiple processes | Multiple machines | Idle pickup |
|---|---|---|---|---|
| `SQLiteStorage("jobs.db")` | one machine, zero setup | ✅ | ❌ | polling (≤ 1 s) |
| `PostgreSQLStorage("postgresql://…")` | production; you already run Postgres | ✅ | ✅ | instant (LISTEN/NOTIFY) |
| `RedisStorage("redis://…")` | production; highest throughput | ✅ | ✅ | instant (pub/sub) |
| `SQLServerStorage("mssql://…")` | production; you already run SQL Server / Azure SQL | ✅ | ✅ | polling (≤ 1 s) |
| `MySQLStorage("mysql://…")` | production; you already run MySQL 8+ / MariaDB 10.6+ | ✅ | ✅ | polling (≤ 1 s) |
| `MemoryStorage()` | tests, demos, single-process apps | ❌ | ❌ | instant |

```python
from schedulefire import configure, PostgreSQLStorage, RedisStorage, storage_from_url

configure(storage=PostgreSQLStorage("postgresql://app:secret@db:5432/app"))   # tables go in schema "schedulefire"
configure(storage=RedisStorage("redis://:secret@cache:6379/0"))
configure(storage=MySQLStorage("mysql://app:secret@db:3306/app"))            # tables prefixed "schedulefire_"
configure(storage=SQLServerStorage("mssql://app:secret@sql:1433/app"))       # tables in schema "schedulefire"
configure(storage=SQLServerStorage(                                           # or any ODBC connection string
    "Driver={ODBC Driver 18 for SQL Server};Server=sql;Database=app;Trusted_Connection=yes;Encrypt=yes"))
configure(storage=storage_from_url(os.environ["SCHEDULEFIRE_DB"]))             # choose via config
```

- **PostgreSQL** creates its tables on first start, in their own schema (`schema="schedulefire"`).
  Workers fetch jobs with `FOR UPDATE SKIP LOCKED`, so hundreds of workers don't block each other.
- **Redis** needs version 5 or later, standalone or Sentinel. Pass `client=` to use an existing
  Sentinel or TLS client. Every key shares the `{schedulefire}` hash tag, so Redis Cluster works too.
  Turn on persistence (AOF) and set `maxmemory-policy noeviction`: a Redis that evicts keys silently loses jobs.
- **SQL Server** (2016+, Azure SQL, LocalDB) creates its tables in their own schema. Workers fetch jobs
  with `READPAST, UPDLOCK`, like Hangfire.SqlServer does. It needs pyodbc and Microsoft's ODBC driver 17 or 18.
  With no user name in the `mssql://` URL, Windows authentication is used. Other options (`Encrypt`,
  `TrustServerCertificate`, ...) can be passed as URL query parameters.
- **MySQL** needs 8.0 or later (MariaDB 10.6 or later) for `SKIP LOCKED`. Tables are created in the
  connected database with a `schedulefire_` prefix (change it with `table_prefix=`).
  Sessions use READ COMMITTED isolation, so workers don't block each other on InnoDB gap locks.
- Under heavy load, SQL Server and MySQL may abort a transaction to resolve a deadlock.
  scheduleFire retries such transactions automatically.
- **SQLite** is safe for many processes on *one* machine. Don't put the file on a network share.

### Setup is automatic

You only provide a connection URL. On first start, scheduleFire creates everything it needs:

| Storage | Created automatically |
|---|---|
| SQLite | the database file and any missing folders, plus tables and indexes |
| PostgreSQL | the database (if missing), the `schedulefire` schema, tables and indexes |
| MySQL / MariaDB | the database (if missing, as `utf8mb4`), plus `schedulefire_*` tables and indexes |
| SQL Server | the database (if missing), the `schedulefire` schema, tables and indexes |
| Redis / Memory | nothing is needed; keys are created as they're used |

- It's safe for several processes to start at the same time: setup is guarded against running twice.
- **Creating the database needs permission.** On PostgreSQL that's `CREATEDB`; on MySQL and SQL Server
  it's `CREATE DATABASE`. Production accounts often don't have it. In that case, startup fails with a message
  containing the exact `CREATE DATABASE ...` statement to run once. After that, the tables are still created
  automatically. To turn the database step off, pass `create_database=False`.

### Supported versions

At startup, each storage checks the server version. If it's too old, you get an
`UnsupportedDatabaseVersionError` that names the version it needs, not an obscure SQL error later.

| Storage | Minimum version | Tested against |
|---|---|---|
| SQLite | 3.8 | 3.50 |
| PostgreSQL | 9.5 | 10, 12, 17 |
| MySQL | 5.7 | 5.7, 8.4 |
| MariaDB | 10.3 | 10.5, 11.4 |
| SQL Server / Azure SQL | 2012 | not yet verified against a live server |
| Redis | 4.0 | 4.0, 5.0 |

Newer servers use better features where they exist. For example, MySQL 8.0+ and MariaDB 10.6+ fetch jobs with
`SKIP LOCKED`. MySQL 5.7 and older MariaDB use a different claiming method that is also exactly-once.
It's verified with 2,000 jobs across two worker processes, with zero duplicates.

### What happens with no database?

**SQLite is used automatically.** If you never call `configure()`, scheduleFire creates a
SQLite file called `schedulefire.db` in the working directory on first use, and logs a warning with its full path.
SQLite ships with Python, so there's nothing to install and no server to run. Jobs survive restarts.

```python
from schedulefire import BackgroundJob
BackgroundJob.enqueue(send_email, "a@b.com")      # no configure() -> ./schedulefire.db
```
```bash
python -m schedulefire server                     # same default file -> picks the job up
```

Every process that shares jobs must use the **same file**. Start the web app and the worker from
the same folder, or point both at one place without code changes:

```bash
export SCHEDULEFIRE_DB=/var/lib/myapp/jobs.db           # or postgresql://... / redis://... / memory://
```

The order of precedence is: `configure(storage=...)` first, then `SCHEDULEFIRE_DB`, then `./schedulefire.db`.

`MemoryStorage()` is the option for "no storage at all". It keeps jobs in RAM, inside one process only,
and loses them when the process stops. It's useful for tests and demos.

## Writing jobs

The job is stored as *module + function name + JSON arguments*. The worker imports the
function by name. That means:

- **Functions must be importable.** Use module-level functions, `@staticmethod`s and
  `@classmethod`s, or instance methods passed through their class (`InvoiceService.generate`).
  For instance methods, the worker creates the object with the `JobActivator`.
  Lambdas, nested functions and bound methods (`obj.method`) are rejected when you enqueue.
- **Arguments must be JSON-serializable.** The serializer also accepts `datetime`, `date`, `time`,
  `timedelta`, `Decimal`, `UUID`, `bytes` and `set`. Tuples come back as lists. Pass IDs rather than
  whole objects, as you would in Hangfire.
- **`async def` jobs work.** They run with `asyncio.run` inside the worker thread.
- **The return value is stored.** It appears in the Succeeded state and on the dashboard.
- **Jobs should be idempotent.** Delivery is at-least-once: a job can run again after a crash.

```python
from schedulefire import queue, AutomaticRetry, DisableConcurrentExecution, get_job_context, Job

@queue("critical")
@AutomaticRetry(attempts=5, delays=[10, 60, 300])        # or except_on=(ValueError,), on_attempts_exceeded="Delete"
def charge_card(order_id: int) -> None: ...

@DisableConcurrentExecution(timeout=600)
def rebuild_index() -> None:
    ctx = get_job_context()
    for batch in batches():
        ctx.throw_if_cancellation_requested()             # stops on shutdown or when deleted from the dashboard
        process(batch)

# To pass options, build a Job explicitly:
BackgroundJob.enqueue(Job(send_email, args=("a@b.com",), kwargs={"subject": "Hi"}, queue="low"))
RecurringJob.add_or_update("sync", Job(sync_account, args=(42,)), "*/15 * * * *", time_zone="Europe/Paris")
```

Continuations that run no matter how the parent finished:

```python
from schedulefire import AwaitingState, ContinuationOptions
BackgroundJob.create(Job(cleanup), AwaitingState(parent_id, ContinuationOptions.ON_ANY_FINISHED_STATE))
```

## How it works

```
Client ──create──► Storage ◄──fetch/update── BackgroundJobServer
                   (SQLite / PostgreSQL /
                    Redis / Memory)
                     jobs, queues,                      ├─ N worker threads
                     history, recurring,                ├─ delayed-job scheduler (Scheduled → Enqueued)
                     servers, locks                     ├─ recurring-job scheduler (cron → Enqueued)
                                                        ├─ heartbeat + dead-server watchdog
                                                        └─ expiration manager (cleans finished jobs)
```

- **States** follow Hangfire: `Enqueued → Processing → Succeeded | Failed`, plus `Scheduled`,
  `Awaiting` (continuations) and `Deleted`. Every transition is recorded in the job's history.
- **Retries.** A global `AutomaticRetry(attempts=10)` filter turns `Failed` into `Scheduled`, with the same
  back-off curve as Hangfire. When the attempts run out, the job stays `Failed`, and failed jobs never expire.
- **Crash recovery.** A worker reserves a job. The server's heartbeat keeps that reservation alive.
  If the process dies, the job becomes visible to other workers again after `invisibility_timeout`
  (30 minutes by default).
- **Several servers**, whether threads or processes, can share one database. The schedulers use
  distributed locks, so each job is scheduled only once.
- **Expiration.** Succeeded and Deleted jobs are removed after `job_expiration` (1 day by default).
  The dashboard keeps all-time totals for them.
- **Graceful shutdown.** On Ctrl+C or SIGTERM, `stop()` signals cancellation, waits `shutdown_timeout`, then
  requeues any jobs that stopped cooperatively.
- **Workers are threads.** Threads suit I/O-bound jobs. For CPU-bound work, run several server processes
  against the same database.

## Changing workers and queues while running

A server's worker count and queues can be changed live, with no restart:

- **Dashboard:** on the Servers page, click **Edit** on a server, then set workers and queues.
- **From the server's own process:** `server.set_worker_count(40)` or `server.set_queues(["critical", "default"])`.
- **From any other process** (another app, a script, an admin command):
  `schedulefire.update_server(server_id, worker_count=40, queues=["critical", "default"])`.
  The server applies it within `control_poll_interval` (5 seconds by default).

When a server scales down, the workers it removes **finish their current job first**. Queue changes
take effect on each worker's next fetch. Until a server applies a change, the dashboard marks it as
*pending*. Changes last until that server process stops. To make them permanent, update the
startup flags (`--workers`, `--queues`).

## Dashboard

The dashboard shows:

- live counts per state and a throughput chart (last 24 hours or 7 days)
- job lists with bulk requeue and delete, one-click retry and pagination
- job details: arguments, result, exception and stack trace, and a timeline of every state change
- recurring jobs with plain-English schedules ("Every day at 02:30"), "Trigger now", and a friendly schedule editor (pick "Weekly", a day and a time - no cron syntax required, though a "Custom" cron field is still there for anything more elaborate)
- rescheduling a pending scheduled job to run sooner ("in 20 minutes") or at a specific date and time
- server health and queues
- jump to a job by ID, auto-refresh every 5 seconds, light/dark theme, and a layout that works on phones

Everything is served by the app itself, with no CDN, so it works offline and behind firewalls.

```python
from schedulefire.dashboard import run_dashboard, DashboardApp

run_dashboard(port=8080)                        # http://127.0.0.1:8080/schedule-fire/
run_dashboard(port=8080, path="/admin/jobs")    # any other path; "/" for the site root
app = DashboardApp(storage)                      # a WSGI app to mount anywhere (see the next section)
```

### Authentication

With no authentication configured, the dashboard is open to **anyone who can reach it**. The standalone
server only listens on `127.0.0.1` unless you change `host`, and it logs a warning when it's reachable from
the network without a login. Choose a mode with `auth=`:

| `auth=` | Who gets in |
|---|---|
| `"none"` (default) | anyone who can reach the dashboard |
| `"local"` | only browsers on the same machine; requests through a reverse proxy are refused |
| `BasicAuth(...)` | users with a name and password, entered on a proper sign-in page |
| `OAuth(...)` | people who sign in with Google, Microsoft, GitHub or any OpenID Connect provider (Okta, Auth0, Keycloak, ...) and are on your allowlist |

```python
from schedulefire.dashboard import BasicAuth, OAuth, hash_password

# Basic: one or more users, with a real HTML sign-in form (not the browser's native popup).
# Store hashes, not passwords (`schedulefire hash-password` prints one).
DashboardApp(storage, auth=BasicAuth({"admin": "pbkdf2_sha256$600000$...", "ops": os.environ["OPS_PW"]}))

# OAuth: register https://<host><path>/auth/callback as the redirect URI at the provider.
DashboardApp(storage, auth=OAuth(
    "google",                                   # "google" | "microsoft" | "github" | "oidc"
    client_id=os.environ["OAUTH_CLIENT_ID"],
    client_secret=os.environ["OAUTH_CLIENT_SECRET"],
    allowed_domains=["example.com"],            # and/or allowed_emails=[...], allowed_users=[...]
    secret_key=os.environ["DASHBOARD_SECRET_KEY"],  # signs sessions; the same in every process
))
OAuth("microsoft", tenant="<tenant id>", ...)          # Microsoft Entra ID
OAuth("oidc", issuer="https://login.example.com", ...)   # any OpenID Connect provider
```

**A request that already carries `Authorization: Basic` (`curl -u`, scripts, monitoring) is still
accepted directly** - no cookie, no idle timeout, since credentials are resent on every request and
there is no session to expire. Only a browser without one gets the sign-in form.

**Idle sign-out**, for `BasicAuth` and `OAuth`: a browser session signs out after `idle_timeout` of
inactivity (5 minutes by default; `None`/`0` disables it). A warning toast counts down before it
happens, with a "Stay signed in" button. Only real navigation and actions count as activity - the
dashboard's own background "Live" auto-refresh never extends a session by itself. `session_lifetime`
(OAuth: 8 hours; BasicAuth's form: also 8 hours) is a separate, absolute cap regardless of activity.

```python
BasicAuth({"admin": "..."}, idle_timeout=900)          # 15 minutes
OAuth("google", ..., idle_timeout=None)                # never sign out from inactivity
```
```toml
[dashboard]
idle_timeout = "15m"           # applies to basic or oauth; "0" disables it
```

How OAuth sign-in works:

- **The sign-in flow** is the standard authorization-code flow with PKCE.
- **Sessions** are kept in a signed, `HttpOnly` cookie for `session_lifetime` (8 hours by default). No
  server-side session store is needed, and it works across processes and restarts.
- **An allowlist is required.** Only verified email addresses count toward `allowed_emails` and
  `allowed_domains`. The dashboard refuses to start without an allowlist, unless you pass
  `allow_any_user=True`.
- **The UI** shows the signed-in user and a **Sign out** button. Actions are logged together with the user's name.

The same settings in `schedulefire.toml`:

```toml
[dashboard]
auth = "oauth"                               # "none" | "local" | "basic" | "oauth"

[dashboard.oauth]
provider = "google"
client_id = "${OAUTH_CLIENT_ID}"
client_secret = "${OAUTH_CLIENT_SECRET}"
secret_key = "${DASHBOARD_SECRET_KEY}"
allowed_domains = ["example.com"]

# or: auth = "basic" with
# [dashboard.users]
# admin = "pbkdf2_sha256$600000$..."
```

### Routing and custom pages

You can change any page's URL, switch optional pages off, and add your own pages. Links, redirects and forms
all follow your routes automatically.

```python
app = DashboardApp(storage, routes={
    "recurring": "/schedules",          # rename a page
    "job": "/tasks/{id}",               # keep the placeholder
    "jobs": "/list/{state}",
    "servers": None,                    # switch off: recurring, servers, queues or search
})

@app.page("/team", "Team stats", icon="servers")     # a custom page inside the dashboard layout
def team(ctx):                                        # ctx.storage, ctx.query, ctx.user, ctx.url(), ctx.escape()
    return f"<p>Waiting: {ctx.storage.count_jobs('Enqueued')}</p>"
```

| Page | Default route | Can be disabled |
|---|---|---|
| overview | `/` | no |
| jobs | `/jobs/{state}` | no |
| job | `/job/{id}` | no |
| recurring | `/recurring` | yes |
| servers | `/servers` | yes |
| queues | `/queues` | yes |
| search | `/search` | yes |
| login, callback, logout | `/auth/login`, `/auth/callback`, `/auth/logout` | no |

The dashboard lives under **`/schedule-fire`** by default, in the standalone server and when mounted in your
app, so its pages never collide with your own routes on the same host. In the config file, change it with
`[dashboard] path = "/admin/jobs"` and customize pages with a `[dashboard.routes]` table (`servers = false`
switches a page off). Custom pages are protected by the same authentication as the rest of the dashboard.

Custom pages, and a pre-built `DashboardApp` in general, need to be handed to `run_dashboard`/`mount_dashboard`
explicitly via `dashboard=`; passing `storage` alone makes them build their own empty `DashboardApp`, so
anything you configured beforehand wouldn't show up:

```python
app = DashboardApp(storage)

@app.page("/stats", "Team stats")
def stats(ctx):
    return f"<p>Waiting: {ctx.storage.count_jobs('Enqueued')}</p>"

run_dashboard(storage, port=8080, dashboard=app)   # serve THIS app, not a fresh empty one
```

### Editing recurring jobs and rescheduling

From the **Recurring jobs** page, each row has an **Edit** button that opens a schedule editor: pick a
frequency (every few minutes, hourly, daily, weekly, monthly, or "Custom" for a raw cron expression), then
plain fields for the time, day or interval - no cron syntax required for the common cases. It's pre-filled
from the job's current schedule whenever that schedule matches one of those common cases; anything more
elaborate falls back to "Custom" with the cron expression shown as-is. Time zone and queue are free-text
fields with suggestions (common IANA zones, and this dashboard's known queues), so anything valid still
works. The job function and its arguments stay whatever they were set to when the recurring job was created.

From a job's own detail page, a pending **Scheduled** job can be rescheduled either relatively ("in 20
minutes") or to an exact date and time (UTC), without re-enqueueing it. Both actions are disabled when the
dashboard is opened with `read_only=True`.

## Using it with web frameworks and other apps

The client (`BackgroundJob`, `RecurringJob`) is plain Python, so it works from any program:
FastAPI, Flask, Django, scripts, CLIs and notebooks. Each framework differs only in how
you mount the dashboard and where the workers run.

| App type | Enqueue jobs | Dashboard | Example |
|---|---|---|---|
| **Flask** / Bottle / Pyramid (WSGI) | in any view | `app.wsgi_app = mount_dashboard(app.wsgi_app)` → `/schedule-fire` | [examples/flask_app.py](examples/flask_app.py) |
| **Django** | in views (use `transaction.on_commit`) | `application = mount_dashboard(get_wsgi_application())` | [examples/django_example.md](examples/django_example.md) |
| **FastAPI** / Starlette (ASGI) | in `def` endpoints (see below) | `app.mount("/schedule-fire", a2wsgi.WSGIMiddleware(DashboardApp(storage)))` | [examples/fastapi_app.py](examples/fastapi_app.py) |
| Scripts, CLIs, notebooks | anywhere | `python -m schedulefire dashboard` | [examples/quickstart.py](examples/quickstart.py) |

**Where to run the workers.** The recommended setup is a separate process:
`python -m schedulefire server --db jobs.db` (or a Django management command).
You can also start a `BackgroundJobServer` inside the web app, for example in FastAPI's `lifespan`,
but every web process then runs its own job server:

- `gunicorn -w 4` starts four job servers.
- Dev-server auto-reloaders can start a second one.

Fork-based servers are safe either way: each process opens its own database connection.

**Async apps.** `enqueue()` does a short, synchronous database write. That is fine in `def`
endpoints: FastAPI runs them in a thread pool. Inside an `async def` endpoint, call it
with `await run_in_threadpool(BackgroundJob.enqueue, f, x)` to avoid blocking the event loop
under load. Jobs themselves can be `async def`.

**Django.** Pass model IDs, not model instances. Jobs that use the ORM must run in a worker
process where Django is set up. A management command does that for you.

## Custom filters and storage

```python
from schedulefire import JobFilter, GlobalJobFilters

class LogEverything(JobFilter):
    def on_performing(self, ctx): print("start", ctx.job_id)
    def on_performed(self, ctx):  print("end", ctx.job_id, ctx.exception)
    def on_state_election(self, ctx): ...   # may replace ctx.candidate_state
    def on_creating(self, ctx): ...         # may set ctx.canceled = True

GlobalJobFilters.add(LogEverything())
```

Custom metadata can be attached to a job with a job parameter - from inside the job with
`get_job_context().set_job_parameter(name, value)`, or from outside it with
`BackgroundJob.set_parameter(job_id, name, value)` (Hangfire's `SetJobParameter`/`GetJobParameter`).
`AutomaticRetry` uses this itself to track `RetryCount`; a scheduled job that's really a retry is
marked with a small "retry N" tag on the Scheduled page.

A custom recurring task that should run inside the server - alongside its workers, schedulers and
heartbeat - is a `BackgroundProcess` (Hangfire's `IBackgroundProcess`):

```python
from schedulefire import BackgroundProcess, BackgroundJobServer

class ReindexSearch(BackgroundProcess):
    interval = 300.0                    # seconds between calls

    def execute(self, storage):
        ...

BackgroundJobServer(background_processes=[ReindexSearch()]).run()
```

To support another database (MySQL, MongoDB, ...), implement `schedulefire.storage.JobStorage`.
The contract for each method is in its docstring. `SQLiteStorage` and `PostgreSQLStorage` are the reference
implementations. You can run the whole test suite against your backend by adding it to `BACKENDS` in
`tests/test_integration.py`.

## Security

### Who can run what

A job is stored as *module + function name + arguments*. **Anyone who can write to the job storage can
make your workers call functions.** So treat the database or Redis as trusted infrastructure, and let
scheduleFire limit *which* functions can run:

- **Standard-library functions are always refused** (`os.system`, `subprocess.run`, `builtins.eval`, ...),
  as is scheduleFire's own code, unless you set `allow_stdlib_jobs`.
- **Other refusals:** classes (only functions and methods are job targets) and dotted names that
  reach through `__dunder__` attributes.
- **Enforcement happens on the worker, right before running.** A job planted directly in the database is
  refused, fails immediately without retries, and is logged as `SECURITY: refused to run job ...`.
- **For production, add an allowlist** so only your own job modules can run:

```toml
[security]
allowed_job_modules = ["myapp.jobs", "billing"]    # these modules and their submodules only
```
```python
schedulefire.configure(job_security=JobSecurityPolicy(allowed_modules=["myapp.jobs", "billing"]))
```

### Dashboard

| Protection | What it does |
|---|---|
| Authentication | `"none"` (open), `"local"`, Basic (PBKDF2-hashed passwords) or OAuth/OpenID Connect with PKCE and signed sessions. See [Authentication](#authentication). In `"local"` mode, requests through a reverse proxy (`X-Forwarded-For`, `Forwarded`, ...) count as remote, so nginx or IIS in front of the dashboard can't expose it by accident. That was a known Hangfire pitfall. |
| Login throttling | 10 wrong passwords from one address within 5 minutes leads to `429 Too Many Requests`. Failed logins are logged. |
| CSRF protection | Every action needs a token that matches its `HttpOnly`, `SameSite=Strict` cookie. Cross-site `Origin`/`Referer` headers are refused. It works across processes and restarts. |
| XSS defenses | All output is escaped. A strict `Content-Security-Policy` allows only the dashboard's own script. |
| Clickjacking | `X-Frame-Options: DENY` and `frame-ancestors 'none'` |
| Open redirects | After an action, only same-site paths are followed. |
| Read-only mode | `read_only=True` hides and rejects every action. |

**With no authentication configured, anyone who can reach the dashboard can see and manage your jobs.**
Before exposing it beyond localhost, enable `basic` or `oauth`. The built-in server speaks plain HTTP. **To expose the dashboard beyond localhost, put it behind an HTTPS
reverse proxy**, or mount it in your web app that already uses HTTPS, and set a login. It logs a warning
when it's reachable from the network without one.

### Storage and secrets

- **Passwords:** keep them out of the config file with `password = "${DB_PASSWORD}"`. `config check` and all
  error messages hide passwords. A warning is logged when a config file contains a literal password.
- **SQLite:** new database files are created as owner-only (`0600`) on Linux and macOS.
- **Database accounts:** give scheduleFire its own account. It needs to read and write its own tables. It only
  needs CREATE permission for the first start, or grant none and create the database once yourself.
- **Redis:** require a password (`requirepass` or ACLs), use `rediss://` (TLS) across networks, and never
  expose the port to the internet.
- **Job data:** arguments and results are stored unencrypted, and failed jobs show their stack trace in the
  dashboard. Pass IDs rather than secrets or personal data as job arguments.

## Python versions

| Python | Supported | Notes |
|---|---|---|
| 3.11 – 3.14 | ✅ | no dependencies |
| 3.9 – 3.10 | ✅ | pip adds `tomli`, for TOML config files |
| 3.7 – 3.8 | ✅ | pip adds `tomli` and `backports.zoneinfo`, for named time zones |
| 3.6 and older, 2.x | ❌ | missing language features the library is built on (`dataclasses`, `contextvars`, `asyncio.run`); these versions are years past end-of-life |

The optional database drivers also support Python 3.7. pip picks a compatible driver version for your Python.
For example, it picks psycopg 3.1 on 3.7 and the latest psycopg on newer versions.

## Tests

```bash
python -m unittest discover -s tests -t .       # SQLite + memory

# also the server databases (use throwaway databases: the tests create and drop their own tables)
SCHEDULEFIRE_TEST_POSTGRES=postgresql://postgres@localhost:5432/postgres \
SCHEDULEFIRE_TEST_REDIS=redis://localhost:6379/15 \
SCHEDULEFIRE_TEST_MYSQL=mysql://root:pw@localhost:3306/schedulefire_test \
SCHEDULEFIRE_TEST_SQLSERVER="Driver={ODBC Driver 18 for SQL Server};Server=localhost;Database=schedulefire_test;UID=sa;PWD=...;TrustServerCertificate=yes" \
python -m unittest discover -s tests -t .
```

## Project layout

```
schedulefire/
  client.py          BackgroundJob, RecurringJob, BackgroundJobClient, RecurringJobManager
  server.py          BackgroundJobServer, workers, schedulers
  states.py          job states
  state_machine.py   state transitions + continuations
  filters.py         JobFilter, AutomaticRetry, DisableConcurrentExecution, GlobalJobFilters
  job.py             Job, @queue, JobActivator
  cron.py            CronExpression, Cron helpers
  settings.py        config file loading (schedulefire.toml / .json)
  context.py         get_job_context(), cancellation
  dashboard/         WSGI dashboard (app, components, built-in CSS/JS)
  storage/           JobStorage interface; sqlite, postgres, mysql, sqlserver, redis, memory
examples/            tasks.py, quickstart.py, flask_app.py, fastapi_app.py, django_example.md
tests/
```

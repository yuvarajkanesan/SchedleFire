import io
from urllib.parse import quote
import os
import tempfile
import threading
import time
import unittest
import uuid
from datetime import timedelta

from schedulefire import (
    AwaitingState,
    BackgroundJobClient,
    BackgroundJobServer,
    BackgroundProcess,
    ContinuationOptions,
    Job,
    MemoryStorage,
    MySQLStorage,
    PostgreSQLStorage,
    RecurringJobManager,
    RedisStorage,
    SQLiteStorage,
    SQLServerStorage,
    update_server,
)
from schedulefire.dashboard import DashboardApp
from schedulefire.states import EnqueuedState, ScheduledState
from tests import jobs
from tests._compat import TemporaryDirectory


def wait_until(predicate, timeout=10.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


# -- storage backends under test ---------------------------------------------------
# SQLite and memory always run. The others run when these are set, e.g.
#   SCHEDULEFIRE_TEST_POSTGRES=postgresql://postgres@127.0.0.1:5432/postgres
#   SCHEDULEFIRE_TEST_REDIS=redis://127.0.0.1:6379/15
#   SCHEDULEFIRE_TEST_MYSQL=mysql://root:pw@127.0.0.1:3306/schedulefire_test
#   SCHEDULEFIRE_TEST_SQLSERVER="Driver={ODBC Driver 18 for SQL Server};Server=...;Database=...;..."
POSTGRES_DSN = os.environ.get("SCHEDULEFIRE_TEST_POSTGRES")
REDIS_URL = os.environ.get("SCHEDULEFIRE_TEST_REDIS")
MYSQL_URL = os.environ.get("SCHEDULEFIRE_TEST_MYSQL")
SQLSERVER = os.environ.get("SCHEDULEFIRE_TEST_SQLSERVER")


def make_sqlite(test):
    tmp = TemporaryDirectory()
    test.addCleanup(tmp.cleanup)
    return SQLiteStorage(os.path.join(tmp.name, "jobs.db"))


def make_memory(test):
    return MemoryStorage()


def make_postgres(test):
    import psycopg

    schema = "t_" + uuid.uuid4().hex[:12]

    def drop():
        with psycopg.connect(POSTGRES_DSN, autocommit=True) as conn:
            conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")

    test.addCleanup(drop)
    storage = PostgreSQLStorage(POSTGRES_DSN, schema=schema)
    test.addCleanup(storage.dispose)
    return storage


def make_redis(test):
    storage = RedisStorage(REDIS_URL, prefix="{t" + uuid.uuid4().hex[:12] + "}")

    def drop():
        keys = list(storage.r.scan_iter(match=storage.prefix + ":*", count=1000))
        if keys:
            storage.r.delete(*keys)
        storage.dispose()

    test.addCleanup(drop)
    return storage


def make_mysql(test):
    prefix = "t" + uuid.uuid4().hex[:10] + "_"
    storage = MySQLStorage(MYSQL_URL, table_prefix=prefix)

    def drop():
        storage.close()
        conn = storage._connect()
        try:
            with conn.cursor() as c:
                c.execute("SHOW TABLES")
                for (name,) in c.fetchall():
                    if name.startswith(prefix):
                        c.execute(f"DROP TABLE `{name}`")
        finally:
            conn.close()

    test.addCleanup(drop)
    return storage


def make_sqlserver(test):
    schema = "t_" + uuid.uuid4().hex[:12]
    storage = SQLServerStorage(SQLSERVER, schema=schema)

    def drop():
        storage.close()
        conn = storage._connect()
        try:
            c = conn.cursor()
            tables = [r[0] for r in c.execute(
                "SELECT t.name FROM sys.tables t WHERE t.schema_id = SCHEMA_ID(?)", schema).fetchall()]
            for name in tables:
                c.execute(f"DROP TABLE [{schema}].[{name}]")
            c.execute(f"DROP SCHEMA [{schema}]")
        finally:
            conn.close()

    test.addCleanup(drop)
    return storage


BACKENDS = [
    ("SQLite", make_sqlite, None),
    ("Memory", make_memory, None),
    ("Postgres", make_postgres, None if POSTGRES_DSN else "set SCHEDULEFIRE_TEST_POSTGRES to run"),
    ("Redis", make_redis, None if REDIS_URL else "set SCHEDULEFIRE_TEST_REDIS to run"),
    ("MySQL", make_mysql, None if MYSQL_URL else "set SCHEDULEFIRE_TEST_MYSQL to run"),
    ("SQLServer", make_sqlserver, None if SQLSERVER else "set SCHEDULEFIRE_TEST_SQLSERVER to run"),
]


class StorageTestCase(unittest.TestCase):
    make_storage = staticmethod(make_sqlite)

    def setUp(self):
        jobs.reset()
        self.storage = self.make_storage(self)
        self.client = BackgroundJobClient(self.storage)
        # Cleanups run last-in first-out, so servers started in a test stop before this.
        self.addCleanup(self.storage.close)

    def state(self, job_id):
        record = self.storage.get_job(job_id)
        return record.state_name if record else None

    def wait_state(self, job_id, state, timeout=10.0):
        ok = wait_until(lambda: self.state(job_id) == state, timeout)
        self.assertTrue(ok, f"job #{job_id} is {self.state(job_id)}, expected {state}")

    def start_server(self, **options):
        options.setdefault("worker_count", 4)
        options.setdefault("queue_poll_interval", 0.1)
        options.setdefault("schedule_polling_interval", 0.2)
        options.setdefault("shutdown_timeout", 5)
        server = BackgroundJobServer(self.storage, **options).start()
        self.addCleanup(server.stop)
        return server


class StorageTests:
    def test_queue_priority_and_invisibility(self):
        low = self.client.enqueue_to("low", jobs.add, 1, 1)
        high = self.client.enqueue_to("high", jobs.add, 2, 2)
        fetched = self.storage.fetch_next(["high", "low"])
        self.assertEqual(fetched.job_id, high)
        self.assertEqual(self.storage.fetch_next(["high", "low"]).job_id, low)
        self.assertIsNone(self.storage.fetch_next(["high", "low"]))
        # A worker that died without finishing: the job becomes visible again.
        self.storage.invisibility_timeout = 0
        time.sleep(0.01)
        self.assertIsNotNone(self.storage.fetch_next(["high"]))

    def test_locks(self):
        lock = self.storage.try_acquire_lock("r", ttl=60)
        self.assertIsNotNone(lock)
        self.assertIsNone(self.storage.try_acquire_lock("r"))
        lock.release()
        self.assertIsNotNone(self.storage.try_acquire_lock("r"))
        expired = self.storage.try_acquire_lock("short", ttl=0)
        time.sleep(0.01)
        self.assertIsNotNone(expired)
        self.assertIsNotNone(self.storage.try_acquire_lock("short"))

    def test_expiration(self):
        self.storage.job_expiration = 0
        job_id = self.client.enqueue(jobs.add, 1, 2)
        self.client.delete(job_id)
        time.sleep(0.01)
        self.assertEqual(self.storage.delete_expired_jobs(time.time()), 1)
        self.assertIsNone(self.storage.get_job(job_id))


class ServerTests:
    def test_fire_and_forget(self):
        self.start_server()
        job_id = self.client.enqueue(jobs.add, 2, 3)
        self.wait_state(job_id, "Succeeded")
        record = self.storage.get_job(job_id)
        self.assertEqual(record.state_data["result"], 5)
        self.assertIsNotNone(record.expire_at)
        history = [s.name for s in self.storage.get_state_history(job_id)]
        self.assertEqual(history, ["Succeeded", "Processing", "Enqueued"])

    def test_failure_without_retry(self):
        self.start_server()
        job_id = self.client.enqueue(jobs.always_fails, "nope")
        self.wait_state(job_id, "Failed")
        data = self.storage.get_job(job_id).state_data
        self.assertEqual(data["exception_type"], "builtins.ValueError")
        self.assertEqual(data["exception_message"], "nope")
        self.assertIn("Traceback", data["exception_details"])
        self.assertIsNone(self.storage.get_job(job_id).expire_at)  # failed jobs are kept

    def test_automatic_retry(self):
        self.start_server()
        job_id = self.client.enqueue(jobs.flaky, "k", 2)
        self.wait_state(job_id, "Succeeded")
        self.assertEqual(self.storage.get_job(job_id).parameters["RetryCount"], 2)
        states = [s.name for s in self.storage.get_state_history(job_id)]
        self.assertEqual(states.count("Scheduled"), 2)

    def test_retry_exhausted_then_deleted(self):
        self.start_server()
        job_id = self.client.enqueue(jobs.fails_then_deleted)
        self.wait_state(job_id, "Deleted")
        self.assertIn("maximum number of retry", self.storage.get_job(job_id).state_reason)

    def test_delayed_job(self):
        self.start_server()
        job_id = self.client.schedule(jobs.add, timedelta(seconds=0.5), 1, 1)
        self.assertEqual(self.state(job_id), "Scheduled")
        time.sleep(0.2)
        self.assertEqual(self.state(job_id), "Scheduled")
        self.wait_state(job_id, "Succeeded")

    def test_queues(self):
        self.start_server(queues=["critical"])
        default_id = self.client.enqueue(jobs.add, 1, 1)
        critical_id = self.client.enqueue(jobs.critical_task, 7)
        self.wait_state(critical_id, "Succeeded")
        time.sleep(0.3)
        self.assertEqual(self.state(default_id), "Enqueued")

    def test_continuations(self):
        parent = self.client.schedule(jobs.add, 3600, 1, 1)
        child = self.client.continue_job_with(parent, jobs.add, 10, 10)
        any_child = self.client.create(Job(jobs.add, (5, 5)), AwaitingState(parent, ContinuationOptions.ON_ANY_FINISHED_STATE))
        self.assertEqual(self.state(child), "Awaiting")

        self.client.delete(parent)
        self.assertEqual(self.state(child), "Deleted")
        self.assertEqual(self.state(any_child), "Enqueued")

        self.start_server()
        parent2 = self.client.enqueue(jobs.add, 1, 2)
        child2 = self.client.continue_job_with(parent2, jobs.add, 3, 4)
        self.wait_state(child2, "Succeeded")
        # Continuation of an already-finished parent starts immediately.
        child3 = self.client.continue_job_with(parent2, jobs.add, 0, 0)
        self.wait_state(child3, "Succeeded")

    def test_deleted_job_is_not_run(self):
        job_id = self.client.enqueue(jobs.add, 1, 1)
        self.client.delete(job_id)
        self.start_server()
        time.sleep(0.3)
        self.assertEqual(jobs.calls, [])
        self.assertEqual(self.storage.get_queues(), [])

    def test_background_process(self):
        calls = []

        class Ping(BackgroundProcess):
            interval = 0.1

            def execute(self, storage):
                calls.append(storage is self.outer.storage)

        Ping.outer = self
        self.start_server(background_processes=[Ping()])
        self.assertTrue(wait_until(lambda: len(calls) >= 2, timeout=5))
        self.assertTrue(all(calls))

    def test_client_side_job_parameters(self):
        job_id = self.client.enqueue(jobs.add, 1, 1)
        self.assertIsNone(self.client.get_parameter(job_id, "Tag"))
        self.client.set_parameter(job_id, "Tag", "nightly")
        self.assertEqual(self.client.get_parameter(job_id, "Tag"), "nightly")
        self.assertEqual(self.storage.get_job(job_id).parameters["Tag"], "nightly")

    def test_instance_static_class_async_and_context(self):
        self.start_server()
        ids = [
            self.client.enqueue(jobs.Greeter.greet, "ann"),
            self.client.enqueue(jobs.Greeter.static_greet, "bo"),
            self.client.enqueue(jobs.Greeter.class_greet, "cy"),
            self.client.enqueue(jobs.async_job, 21),
            self.client.enqueue(jobs.whoami),
        ]
        for job_id in ids:
            self.wait_state(job_id, "Succeeded")
        self.assertEqual(self.storage.get_job(ids[0]).state_data["result"], "hello ann")
        self.assertEqual(self.storage.get_job(ids[3]).state_data["result"], 42)
        self.assertIn(("whoami", (ids[4], 0), {}), jobs.calls)

    def test_disable_concurrent_execution(self):
        self.start_server(worker_count=4)
        ids = [self.client.enqueue(jobs.exclusive, i) for i in range(3)]
        for job_id in ids:
            self.wait_state(job_id, "Succeeded")
        self.assertEqual(jobs.max_concurrency(), 1)

    def test_shutdown_requeues_cooperative_job(self):
        server = self.start_server(worker_count=1)
        job_id = self.client.enqueue(jobs.cooperative_long_job)
        self.wait_state(job_id, "Processing")
        server.stop()
        self.assertEqual(self.state(job_id), "Enqueued")
        self.assertEqual(self.storage.get_servers(), [])

    def test_delete_while_processing_aborts(self):
        from schedulefire.context import JobContext
        JobContext.state_check_interval = 0.05
        self.addCleanup(setattr, JobContext, "state_check_interval", 5.0)
        self.start_server(worker_count=1)
        job_id = self.client.enqueue(jobs.cooperative_long_job)
        self.wait_state(job_id, "Processing")
        self.client.delete(job_id)
        time.sleep(0.5)
        self.assertEqual(self.state(job_id), "Deleted")
        self.assertNotIn(("cooperative_done", (), {}), jobs.calls)

    def test_recurring_jobs(self):
        manager = RecurringJobManager(self.storage)
        manager.add_or_update("sum", Job(jobs.add, (4, 4)), "*/5 * * * *")
        record = self.storage.get_recurring("sum")
        self.assertGreater(record.next_execution, time.time())

        triggered = manager.trigger("sum")
        self.assertEqual(self.state(triggered), "Enqueued")
        self.assertEqual(self.storage.get_recurring("sum").next_execution, record.next_execution)

        # Make it due; the server's scheduler should enqueue it and move next_execution forward.
        self.storage.update_recurring("sum", next_execution=time.time() - 1)
        self.start_server()
        self.assertTrue(wait_until(lambda: self.storage.get_recurring("sum").last_job_id != triggered))
        rec = self.storage.get_recurring("sum")
        self.assertGreater(rec.next_execution, time.time())
        self.wait_state(rec.last_job_id, "Succeeded")

        with self.assertRaises(ValueError):
            manager.add_or_update("bad", jobs.add, "not a cron")
        self.assertTrue(manager.remove_if_exists("sum"))
        self.assertFalse(manager.remove_if_exists("sum"))

    def test_scale_workers_live(self):
        server = self.start_server(worker_count=2)
        self.assertEqual(server.active_workers, 2)
        server.set_worker_count(5)
        self.assertEqual(server.active_workers, 5)
        ids = [self.client.enqueue(jobs.add, i, 1) for i in range(10)]
        for job_id in ids:
            self.wait_state(job_id, "Succeeded")
        server.set_worker_count(1)
        self.assertEqual(server.active_workers, 1)
        workers = [t for t in server._threads if t.name.startswith("schedulefire-worker")]
        self.assertTrue(wait_until(lambda: sum(t.is_alive() for t in workers) == 1), "retired workers exit")
        self.assertEqual(self.storage.get_server(server.server_id).worker_count, 1)
        job_id = self.client.enqueue(jobs.add, 2, 2)
        self.wait_state(job_id, "Succeeded")
        with self.assertRaises(ValueError):
            server.set_worker_count(0)

    def test_change_queues_live(self):
        server = self.start_server(queues=["critical"])
        job_id = self.client.enqueue(jobs.add, 1, 1)  # "default" queue
        time.sleep(0.3)
        self.assertEqual(self.state(job_id), "Enqueued")
        server.set_queues(["critical", "default"])
        self.wait_state(job_id, "Succeeded")
        self.assertEqual(self.storage.get_server(server.server_id).queues, ["critical", "default"])

    def test_remote_update_from_another_process(self):
        server = self.start_server(worker_count=2, control_poll_interval=0.1)
        self.assertFalse(update_server("no-such-server", worker_count=3, storage=self.storage))
        self.assertTrue(update_server(server.server_id, worker_count=4, storage=self.storage))
        self.assertTrue(update_server(server.server_id, queues="low, default", storage=self.storage))
        self.assertEqual(self.storage.get_server(server.server_id).requested,
                         {"worker_count": 4, "queues": ["low", "default"]})
        self.assertTrue(wait_until(lambda: server.active_workers == 4 and server.queues == ["low", "default"]))
        self.assertTrue(wait_until(lambda: self.storage.get_server(server.server_id).requested is None))
        record = self.storage.get_server(server.server_id)
        self.assertEqual((record.worker_count, record.queues), (4, ["low", "default"]))
        job_id = self.client.enqueue_to("low", jobs.add, 3, 3)
        self.wait_state(job_id, "Succeeded")
        for bad in ({"worker_count": 0}, {"worker_count": 5000}, {"queues": "Bad Queue"}, {"queues": []}):
            with self.assertRaises(ValueError):
                update_server(server.server_id, storage=self.storage, **bad)

    def test_two_servers_share_storage(self):
        self.start_server(worker_count=2)
        self.start_server(worker_count=2)
        ids = [self.client.enqueue(jobs.add, i, i) for i in range(30)]
        for job_id in ids:
            self.wait_state(job_id, "Succeeded")
        self.assertEqual(len([c for c in jobs.calls if c[0] == "add"]), 30)
        self.assertEqual(len(self.storage.get_servers()), 2)


class DashboardTests:
    def request(self, app, path, method="GET", body="", remote="127.0.0.1", cookie=None):
        environ = {
            "HTTP_COOKIE": cookie or "",
            "REQUEST_METHOD": method,
            "PATH_INFO": path.split("?")[0],
            "QUERY_STRING": path.split("?")[1] if "?" in path else "",
            "SCRIPT_NAME": "",
            "REMOTE_ADDR": remote,
            "CONTENT_LENGTH": str(len(body.encode())),
            "wsgi.input": io.BytesIO(body.encode()),
        }
        result = {}

        def start_response(status, headers):
            result["status"] = status
            result["headers"] = dict(headers)

        result["body"] = b"".join(app(environ, start_response)).decode()
        return result

    def csrf(self, app, path="/"):
        """Load a page like a browser does; returns (token, cookie header) for posting forms."""
        set_cookie = self.request(app, path)["headers"]["Set-Cookie"]
        pair = set_cookie.split(";")[0]
        return pair.split("=", 1)[1], pair

    def test_csrf_works_across_processes_and_restarts(self):
        # Two app instances stand in for two worker processes (or a restart): a form
        # rendered by one must be accepted by the other.
        first, second = DashboardApp(self.storage), DashboardApp(self.storage)
        job_id = self.client.enqueue(jobs.add, 1, 1)
        token, cookie = self.csrf(first, f"/job/{job_id}")
        self.assertIn(f'name="csrf" value="{token}"', self.request(first, f"/job/{job_id}", cookie=cookie)["body"])
        self.assertNotIn("Set-Cookie", self.request(first, "/", cookie=cookie)["headers"])  # cookie reused
        res = self.request(second, "/actions/jobs/delete", "POST", f"id={job_id}&next=/&csrf={token}", cookie=cookie)
        self.assertEqual(res["status"], "303 See Other")
        self.assertEqual(self.state(job_id), "Deleted")
        # Rejected: token without its cookie, cookie without a token, mismatched pair.
        other_token, other_cookie = self.csrf(second)
        for body, jar in ((f"id=1&csrf={token}", None), ("id=1", cookie), (f"id=1&csrf={other_token}", cookie)):
            self.assertEqual(self.request(second, "/actions/jobs/requeue", "POST", body, cookie=jar)["status"],
                             "403 Forbidden")
        self.assertIn("HttpOnly; SameSite=Strict", self.request(first, "/")["headers"]["Set-Cookie"])

    def test_redirects_stay_on_site(self):
        app = DashboardApp(self.storage)
        token, cookie = self.csrf(app)
        for target in ("//evil.example", "/\\evil.example", "https://evil.example", "/job/1\r\nX: y"):
            res = self.request(app, "/actions/jobs/requeue", "POST",
                               "id=1&csrf=" + token + "&next=" + quote(target), cookie=cookie)
            self.assertTrue(res["headers"]["Location"].startswith("/?notice="), target)

    def test_pages_and_actions(self):
        app = DashboardApp(self.storage)
        job_id = self.client.enqueue(jobs.add, 1, 2)
        failed_id = self.client.create(Job(jobs.always_fails, ("x",)), EnqueuedState())
        RecurringJobManager(self.storage).add_or_update("r1", jobs.add, "@daily")

        for path in ["/", "/jobs/enqueued", "/jobs/failed", "/jobs/succeeded", f"/job/{job_id}",
                     "/recurring", "/servers", "/queues"]:
            res = self.request(app, path)
            self.assertEqual(res["status"], "200 OK", path)
        self.assertIn("add(1, 2)", self.request(app, f"/job/{job_id}")["body"])
        self.assertEqual(self.request(app, "/job/99999")["status"], "404 Not Found")

        # Missing CSRF token is rejected.
        res = self.request(app, "/actions/jobs/delete", "POST", f"id={job_id}&next=/")
        self.assertEqual(res["status"], "403 Forbidden")
        self.assertEqual(self.state(job_id), "Enqueued")

        token, cookie = self.csrf(app)
        res = self.request(app, "/actions/jobs/delete", "POST", f"id={job_id}&id={failed_id}&next=/jobs/deleted&csrf={token}",
                           cookie=cookie)
        self.assertEqual(res["status"], "303 See Other")
        self.assertTrue(res["headers"]["Location"].startswith("/jobs/deleted?notice="))
        self.assertEqual(self.state(job_id), "Deleted")
        self.assertEqual(self.state(failed_id), "Deleted")

        res = self.request(app, "/actions/recurring/trigger", "POST", f"id=r1&next=/recurring&csrf={token}",
                           cookie=cookie)
        self.assertEqual(res["status"], "303 See Other")
        self.assertIsNotNone(self.storage.get_recurring("r1").last_job_id)

    def test_retry_badge_on_the_scheduled_list(self):
        app = DashboardApp(self.storage)
        job_id = self.client.enqueue(jobs.add, 1, 1)
        self.storage.set_job_parameter(job_id, "RetryCount", 2)
        self.client.change_state(job_id, ScheduledState(60, reason="Retry attempt 2 of 3: boom"))
        self.client.schedule(jobs.add, 3600, 1, 1)  # an ordinary delayed job: no badge

        page = self.request(app, "/jobs/scheduled")["body"]
        self.assertIn('<span class="tag warn">retry 2</span>', page)
        self.assertIn("Retry attempt 2 of 3: boom", page)
        self.assertEqual(page.count('class="tag warn"'), 1)

    def test_mount_dashboard_under_prefix(self):
        from schedulefire.dashboard import mount_dashboard

        def host_app(environ, start_response):
            start_response("200 OK", [("Content-Type", "text/plain")])
            return [b"host:" + environ["PATH_INFO"].encode()]

        app = mount_dashboard(host_app, "/jobs", self.storage)
        self.assertEqual(self.request(app, "/other")["body"], "host:/other")
        self.assertEqual(self.request(app, "/jobsx")["body"], "host:/jobsx")
        page = self.request(app, "/jobs/recurring")
        self.assertEqual(page["status"], "200 OK")
        self.assertIn('href="/jobs/servers"', page["body"])
        RecurringJobManager(self.storage).add_or_update("r", jobs.add, "@daily")
        csrf, cookie = self.csrf(app, "/jobs/")
        res = self.request(app, "/jobs/actions/recurring/trigger", "POST", f"id=r&next=/recurring&csrf={csrf}",
                           cookie=cookie)
        self.assertTrue(res["headers"]["Location"].startswith("/jobs/recurring?notice="))

    def test_edit_server_from_dashboard(self):
        app = DashboardApp(self.storage)
        self.storage.announce_server("srv-1", "web-01", ["default"], 5)
        page = self.request(app, "/servers")["body"]
        self.assertIn('name="workers"', page)
        csrf, cookie = self.csrf(app, "/servers")
        res = self.request(app, "/actions/servers/update", "POST",
                           f"id=srv-1&workers=12&queues=critical%2C+default&csrf={csrf}", cookie=cookie)
        self.assertEqual(res["status"], "303 See Other")
        self.assertNotIn("level=error", res["headers"]["Location"])
        self.assertEqual(self.storage.get_server("srv-1").requested,
                         {"worker_count": 12, "queues": ["critical", "default"]})
        self.assertIn("12 pending", self.request(app, "/servers")["body"])

        res = self.request(app, "/actions/servers/update", "POST", f"id=srv-1&workers=8&queues=Bad+Name&csrf={csrf}",
                           cookie=cookie)
        self.assertIn("level=error", res["headers"]["Location"])
        res = self.request(app, "/actions/servers/update", "POST", f"id=gone&workers=3&queues=default&csrf={csrf}",
                           cookie=cookie)
        self.assertIn("level=error", res["headers"]["Location"])
        self.assertNotIn('name="workers"', self.request(DashboardApp(self.storage, read_only=True), "/servers")["body"])

    def test_html_is_escaped(self):
        app = DashboardApp(self.storage)
        job_id = self.client.enqueue(jobs.add, "<script>alert(1)</script>", "x")
        body = self.request(app, f"/job/{job_id}")["body"]
        self.assertNotIn("<script>alert(1)</script>", body)

    def test_access_control(self):
        # No authentication configured: open to everyone who can reach it.
        self.assertEqual(self.request(DashboardApp(self.storage), "/", remote="10.0.0.5")["status"], "200 OK")
        local = DashboardApp(self.storage, auth="local")
        self.assertEqual(self.request(local, "/", remote="10.0.0.5")["status"], "403 Forbidden")
        self.assertEqual(self.request(local, "/", remote="127.0.0.1")["status"], "200 OK")
        app = DashboardApp(self.storage, basic_auth=("admin", "secret"))
        # A plain browser navigation gets the sign-in form (302); only a raw Authorization
        # header (curl -u, scripts) gets the stateless 401 challenge - see below.
        self.assertEqual(self.request(app, "/", remote="10.0.0.5")["status"], "302 Found")
        import base64
        environ_auth = base64.b64encode(b"admin:secret").decode()
        environ = {"REQUEST_METHOD": "GET", "PATH_INFO": "/", "REMOTE_ADDR": "10.0.0.5",
                   "HTTP_AUTHORIZATION": "Basic " + environ_auth, "wsgi.input": io.BytesIO()}
        statuses = []
        app(environ, lambda s, h: statuses.append(s))
        self.assertEqual(statuses, ["200 OK"])
        read_only = DashboardApp(self.storage, read_only=True)
        self.assertEqual(self.request(read_only, "/actions/jobs/delete", "POST", "id=1")["status"], "403 Forbidden")


class DatabaseCreationTests(unittest.TestCase):
    """Pointing a storage at a database that doesn't exist yet creates it, then the tables."""

    @unittest.skipUnless(MYSQL_URL, "set SCHEDULEFIRE_TEST_MYSQL to run")
    def test_mysql_creates_database(self):
        base, _, _ = MYSQL_URL.rpartition("/")
        name = "sf_new_" + uuid.uuid4().hex[:8]
        storage = MySQLStorage(f"{base}/{name}")
        try:
            job_id = BackgroundJobClient(storage).enqueue(jobs.add, 1, 2)
            self.assertEqual(storage.get_job(job_id).state_name, "Enqueued")
        finally:
            storage.close()
            conn = storage._connect()
            with conn.cursor() as c:
                c.execute(f"DROP DATABASE `{name}`")
            conn.close()

    @unittest.skipUnless(POSTGRES_DSN, "set SCHEDULEFIRE_TEST_POSTGRES to run")
    def test_postgres_creates_database(self):
        import psycopg

        base, _, _ = POSTGRES_DSN.rpartition("/")
        name = "sf_new_" + uuid.uuid4().hex[:8]
        storage = PostgreSQLStorage(f"{base}/{name}")
        try:
            job_id = BackgroundJobClient(storage).enqueue(jobs.add, 1, 2)
            self.assertEqual(storage.get_job(job_id).state_name, "Enqueued")
        finally:
            storage.dispose()
            with psycopg.connect(POSTGRES_DSN, autocommit=True) as admin:
                admin.execute(f'DROP DATABASE "{name}"')

    @unittest.skipUnless(SQLSERVER, "set SCHEDULEFIRE_TEST_SQLSERVER to run")
    def test_sqlserver_creates_database(self):
        import re

        name = "sf_new_" + uuid.uuid4().hex[:8]
        cs = re.sub(r"(?i)(database|initial catalog)\s*=\s*[^;]*", f"Database={name}", SQLSERVER, count=1)
        storage = SQLServerStorage(cs)
        try:
            job_id = BackgroundJobClient(storage).enqueue(jobs.add, 1, 2)
            self.assertEqual(storage.get_job(job_id).state_name, "Enqueued")
        finally:
            storage.close()
            master = re.sub(r"(?i)database\s*=\s*[^;]*", "Database=master", cs, count=1)
            import pyodbc

            admin = pyodbc.connect(master, autocommit=True)
            admin.execute(f"ALTER DATABASE [{name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
            admin.execute(f"DROP DATABASE [{name}]")
            admin.close()

    def test_sqlite_creates_folders(self):
        with TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "nested", "deeper", "jobs.db")
            storage = SQLiteStorage(path)
            try:
                self.assertTrue(os.path.exists(path))
                job_id = BackgroundJobClient(storage).enqueue(jobs.add, 1, 2)
                self.assertEqual(storage.get_job(job_id).state_name, "Enqueued")
            finally:
                storage.close()


# One concrete TestCase per (suite, backend), e.g. ServerTests_Postgres.
for _backend, _factory, _skip in BACKENDS:
    for _suite in (StorageTests, ServerTests, DashboardTests):
        _cls = type(f"{_suite.__name__}_{_backend}", (_suite, StorageTestCase),
                    {"make_storage": staticmethod(_factory), "__module__": __name__})
        globals()[_cls.__name__] = unittest.skip(_skip)(_cls) if _skip else _cls
del _backend, _factory, _skip, _suite, _cls


if __name__ == "__main__":
    unittest.main()

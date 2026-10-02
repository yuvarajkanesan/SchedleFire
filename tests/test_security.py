"""Security tests: the job security policy, dashboard hardening and config secrets."""

import base64
import io
import logging
import os
import stat
import time
import unittest

from schedulefire import (
    BackgroundJobClient,
    BackgroundJobServer,
    Job,
    JobSecurityError,
    JobSecurityPolicy,
    SQLiteStorage,
    configure,
)
from schedulefire.dashboard import DashboardApp
from schedulefire.dashboard import assets
from schedulefire.job import get_job_security_policy, set_job_security_policy
from schedulefire.settings import ConfigError, load_config
from schedulefire.states import EnqueuedState
from tests import jobs
from tests._compat import TemporaryDirectory


def wait_until(predicate, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


class PolicyTests(unittest.TestCase):
    def blocked(self, policy, module, qualname):
        with self.assertRaises(JobSecurityError):
            policy.check(module, qualname)

    def test_default_policy(self):
        policy = JobSecurityPolicy()
        for module, name in [("os", "system"), ("subprocess", "run"), ("builtins", "eval"), ("shutil", "rmtree"),
                             ("importlib", "import_module"), ("pickle", "loads"), ("os.path", "join"),
                             ("schedulefire.config", "configure"), ("tests.jobs", "add.__globals__.get"),
                             ("tests.jobs", "Greeter.__init__"), ("tests.jobs; import os", "x")]:
            with self.subTest(target=f"{module}.{name}"):
                self.blocked(policy, module, name)
        policy.check("tests.jobs", "add")
        policy.check("tests.jobs", "Greeter.greet")
        policy.check("__main__", "main")

    def test_allowlist(self):
        policy = JobSecurityPolicy(["tests.jobs", "examples.*"])
        policy.check("tests.jobs", "add")
        policy.check("examples.tasks", "nightly_report")
        self.blocked(policy, "tests.test_units", "at")
        self.blocked(policy, "tests", "anything")
        self.blocked(policy, "os", "system")
        self.blocked(JobSecurityPolicy(["tests"]), "tests_evil.mod", "f")  # prefix must end at a dot

    def test_opt_ins(self):
        JobSecurityPolicy(allow_stdlib=True).check("json", "dumps")
        self.blocked(JobSecurityPolicy(allow_stdlib=True), "schedulefire.job", "Job")  # never allowed
        self.blocked(JobSecurityPolicy(blocked_modules=["tests.jobs"]), "tests.jobs", "add")

    def test_client_refuses_dangerous_targets(self):
        with self.assertRaises(JobSecurityError):
            Job(os.remove, ("x",))
        with self.assertRaises(TypeError):
            Job(jobs.Greeter)  # classes are not job targets


class WorkerEnforcementTests(unittest.TestCase):
    """An attacker with write access to the job storage inserts jobs directly."""

    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.storage = SQLiteStorage(os.path.join(self.tmp.name, "jobs.db"))
        self.addCleanup(self.storage.close)
        saved = get_job_security_policy()
        self.addCleanup(set_job_security_policy, saved)

    def inject(self, module, qualname, args):
        job_id = self.storage.create_job({"module": module, "qualname": qualname, "args": args, "kwargs": {}},
                                         "default", {}, expire_in=3600)
        self.storage.set_state(job_id, EnqueuedState(queue="default"))
        return job_id

    def run_server(self):
        server = BackgroundJobServer(self.storage, worker_count=2, queue_poll_interval=0.05,
                                     schedule_polling_interval=0.1).start()
        self.addCleanup(server.stop)

    def test_worker_refuses_injected_jobs(self):
        victim = os.path.join(self.tmp.name, "important.txt")
        with open(victim, "w") as f:
            f.write("keep me")
        attacks = [
            self.inject("os", "remove", [victim]),                       # stdlib function
            self.inject("builtins", "exec", ["open(r'%s','w').close()" % victim]),
            self.inject("tests.jobs", "Greeter", []),                    # class, not a function
            self.inject("tests.jobs", "add.__globals__.clear", []),      # dunder traversal
        ]
        with self.assertLogs("schedulefire", "WARNING") as logs:
            self.run_server()
            self.assertTrue(wait_until(lambda: all(self.storage.get_job(j).state_name == "Failed" for j in attacks)))
        with open(victim) as f:
            self.assertEqual(f.read(), "keep me")
        for job_id in attacks:
            record = self.storage.get_job(job_id)
            self.assertEqual(record.state_data["exception_type"], "schedulefire.job.JobSecurityError")
            history = [s.name for s in self.storage.get_state_history(job_id)]
            self.assertNotIn("Scheduled", history, "security refusals must not be retried")
        self.assertTrue(any("SECURITY: refused" in line for line in logs.output))

    def test_allowlist_on_worker(self):
        configure(job_security=JobSecurityPolicy(allowed_modules=["tests.jobs"]))
        ok = self.inject("tests.jobs", "add", [2, 3])
        refused = self.inject("tests.test_security", "wait_until", [None])
        self.run_server()
        self.assertTrue(wait_until(lambda: self.storage.get_job(ok).state_name == "Succeeded"))
        self.assertTrue(wait_until(lambda: self.storage.get_job(refused).state_name == "Failed"))

    @unittest.skipUnless(os.name == "posix", "file modes are POSIX-only")
    def test_sqlite_file_is_private(self):
        mode = stat.S_IMODE(os.stat(os.path.join(self.tmp.name, "jobs.db")).st_mode)
        self.assertEqual(mode, 0o600)


class DashboardSecurityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.storage = SQLiteStorage(os.path.join(self.tmp.name, "jobs.db"))
        self.addCleanup(self.storage.close)
        self.job_id = BackgroundJobClient(self.storage).enqueue(jobs.add, 1, 2)

    def call(self, app, path="/", method="GET", body="", **extra):
        environ = {"REQUEST_METHOD": method, "PATH_INFO": path, "QUERY_STRING": "", "SCRIPT_NAME": "",
                   "REMOTE_ADDR": "127.0.0.1", "HTTP_HOST": "127.0.0.1:8080", "SERVER_NAME": "127.0.0.1",
                   "SERVER_PORT": "8080", "CONTENT_LENGTH": str(len(body)), "wsgi.input": io.BytesIO(body.encode())}
        environ.update(extra)
        out = {}
        body_bytes = b"".join(app(environ, lambda s, h: out.update(status=s, headers=dict(h))))
        out["body"] = body_bytes.decode("utf-8", "replace")
        return out

    def token(self, app):
        pair = self.call(app, f"/job/{self.job_id}")["headers"]["Set-Cookie"].split(";")[0]
        return pair.split("=", 1)[1], pair

    def test_reverse_proxy_does_not_count_as_local(self):
        app = DashboardApp(self.storage, auth="local")
        self.assertEqual(self.call(app)["status"], "200 OK")
        for header in ("HTTP_X_FORWARDED_FOR", "HTTP_FORWARDED", "HTTP_X_REAL_IP"):
            self.assertEqual(self.call(app, **{header: "203.0.113.9"})["status"], "403 Forbidden", header)

    def test_security_headers(self):
        headers = self.call(DashboardApp(self.storage))["headers"]
        csp = headers["Content-Security-Policy"]
        self.assertIn("default-src 'none'", csp)
        self.assertIn("frame-ancestors 'none'", csp)
        self.assertIn(assets.THEME_BOOT_HASH, csp)
        self.assertEqual(headers["X-Frame-Options"], "DENY")
        self.assertEqual(headers["X-Content-Type-Options"], "nosniff")
        page = self.call(DashboardApp(self.storage))["body"]
        self.assertIn(f"<script>{assets.THEME_BOOT}</script>", page)  # the only inline script

    def test_cross_site_posts_are_refused(self):
        app = DashboardApp(self.storage)
        token, cookie = self.token(app)
        body = f"id={self.job_id}&next=/&csrf={token}"
        evil = self.call(app, "/actions/jobs/delete", "POST", body, HTTP_COOKIE=cookie,
                         HTTP_ORIGIN="https://evil.example")
        self.assertEqual(evil["status"], "403 Forbidden")
        for origin in ("null",):
            self.assertEqual(self.call(app, "/actions/jobs/delete", "POST", body, HTTP_COOKIE=cookie,
                                       HTTP_ORIGIN=origin)["status"], "403 Forbidden")
        referer = self.call(app, "/actions/jobs/delete", "POST", body, HTTP_COOKIE=cookie,
                            HTTP_REFERER="https://evil.example/page")
        self.assertEqual(referer["status"], "403 Forbidden")
        self.assertEqual(self.storage.get_job(self.job_id).state_name, "Enqueued")
        ok = self.call(app, "/actions/jobs/delete", "POST", body, HTTP_COOKIE=cookie,
                       HTTP_ORIGIN="http://127.0.0.1:8080")
        self.assertEqual(ok["status"], "303 See Other")
        proxied = self.call(DashboardApp(self.storage, auth="local"), "/actions/jobs/requeue", "POST", body,
                            HTTP_COOKIE=cookie,
                            HTTP_ORIGIN="https://jobs.example.com", HTTP_HOST="127.0.0.1:8080",
                            HTTP_X_FORWARDED_HOST="jobs.example.com",
                            HTTP_AUTHORIZATION="Basic " + base64.b64encode(b"a:b").decode())
        self.assertNotEqual(proxied["status"], "303 See Other")  # proxied requests still need a login

    def test_negative_content_length_is_not_read_unbounded(self):
        app = DashboardApp(self.storage)
        token, cookie = self.token(app)

        class Endless(io.RawIOBase):
            reads = []

            def read(self, n=-1):
                self.reads.append(n)
                if n is None or n < 0:
                    raise AssertionError("unbounded read")
                return b""

        res = self.call(app, "/actions/jobs/requeue", "POST", "", HTTP_COOKIE=cookie, CONTENT_LENGTH="-1")
        environ_input = Endless()
        environ = {"REQUEST_METHOD": "POST", "PATH_INFO": "/actions/jobs/requeue", "REMOTE_ADDR": "127.0.0.1",
                   "CONTENT_LENGTH": "-1", "HTTP_COOKIE": cookie, "wsgi.input": environ_input}
        statuses = []
        app(environ, lambda s, h: statuses.append(s))
        self.assertEqual(statuses, ["403 Forbidden"])  # no token in the (empty) body
        self.assertTrue(all(n is not None and n >= 0 for n in Endless.reads))
        self.assertEqual(res["status"], "403 Forbidden")

    def basic(self, user, password):
        return "Basic " + base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")

    def test_login_with_non_ascii_password(self):
        app = DashboardApp(self.storage, basic_auth=("admin", "pässwörd"))
        self.assertEqual(self.call(app, REMOTE_ADDR="10.0.0.1", HTTP_AUTHORIZATION=self.basic("admin", "wrong€"))
                         ["status"], "401 Unauthorized")
        self.assertEqual(self.call(app, REMOTE_ADDR="10.0.0.1", HTTP_AUTHORIZATION=self.basic("admin", "pässwörd"))
                         ["status"], "200 OK")
        self.assertEqual(self.call(app, REMOTE_ADDR="10.0.0.1", HTTP_AUTHORIZATION="Basic !!notbase64")
                         ["status"], "401 Unauthorized")

    def test_password_guessing_is_throttled(self):
        app = DashboardApp(self.storage, basic_auth=("admin", "secret"))
        with self.assertLogs("schedulefire.dashboard", "WARNING"):
            for i in range(10):
                self.assertEqual(self.call(app, REMOTE_ADDR="10.0.0.66",
                                           HTTP_AUTHORIZATION=self.basic("admin", f"guess{i}"))["status"],
                                 "401 Unauthorized")
        blocked = self.call(app, REMOTE_ADDR="10.0.0.66", HTTP_AUTHORIZATION=self.basic("admin", "secret"))
        self.assertEqual(blocked["status"], "429 Too Many Requests")
        self.assertGreater(int(blocked["headers"]["Retry-After"]), 0)
        other = self.call(app, REMOTE_ADDR="10.0.0.7", HTTP_AUTHORIZATION=self.basic("admin", "secret"))
        self.assertEqual(other["status"], "200 OK")  # other clients are not affected


class ConfigSecurityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        saved = get_job_security_policy()
        self.addCleanup(set_job_security_policy, saved)

    def write(self, text):
        path = os.path.join(self.tmp.name, "schedulefire.toml")
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        return path

    def test_security_section(self):
        path = self.write('[storage.m]\ntype = "memory"\n[security]\nallowed_job_modules = ["tests.jobs"]\n')
        settings = load_config(path, environ={})
        settings.apply()
        self.assertEqual(get_job_security_policy().allowed_modules, ["tests.jobs"])
        self.assertIn("jobs limited to tests.jobs", settings.describe())

    def test_errors_never_echo_secrets(self):
        path = self.write('[storage.pg]\ntype = "postgres"\npassword = 12345678\n')
        with self.assertRaises(ConfigError) as ctx:
            load_config(path, environ={})
        self.assertNotIn("12345678", str(ctx.exception))

    def test_warns_about_passwords_in_the_file(self):
        path = self.write('[storage.pg]\ntype = "postgres"\nurl = "postgresql://app:hunter2@db/app"\n'
                          '[dashboard]\nusername = "admin"\npassword = "letmein"\n')
        with self.assertLogs("schedulefire", "WARNING") as logs:
            load_config(path, environ={})
        message = "\n".join(logs.output)
        self.assertIn("storage.pg.url", message)
        self.assertIn("dashboard.password", message)
        self.assertNotIn("hunter2", message)
        self.assertNotIn("letmein", message)
        clean = self.write('[dashboard]\nusername = "admin"\npassword = "${DASH_PW}"\n')
        logger = logging.getLogger("schedulefire")
        with self.assertRaises(AssertionError):  # no warning for ${ENV} references
            with self.assertLogs(logger, "WARNING"):
                load_config(clean, environ={"DASH_PW": "x"})


if __name__ == "__main__":
    unittest.main()

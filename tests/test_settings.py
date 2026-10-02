import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

from schedulefire import MemoryStorage, SQLiteStorage, config
from schedulefire.filters import AutomaticRetry, GlobalJobFilters
from schedulefire.settings import (
    TEMPLATE,
    ConfigError,
    _mysql_url,
    _postgres_dsn,
    _redis_url,
    _sqlserver_connection,
    load_config,
    mask_secrets,
    parse_duration,
    parse_settings,
)

from tests._compat import TemporaryDirectory

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class ConfigFileCase(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def write(self, text, name="schedulefire.toml"):
        path = os.path.join(self.tmp.name, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        return path


class ParsingTests(ConfigFileCase):
    def test_durations(self):
        self.assertEqual(parse_duration(90, "x"), 90)
        self.assertEqual(parse_duration("500ms", "x"), 0.5)
        self.assertEqual(parse_duration("1h30m", "x"), 5400)
        self.assertEqual(parse_duration("1d", "x"), 86400)
        self.assertEqual(parse_duration("2.5", "x"), 2.5)
        for bad in ("soon", "5 minutes", -1, True, "1x"):
            with self.assertRaises(ConfigError):
                parse_duration(bad, "x")

    def test_template_is_valid(self):
        settings = load_config(self.write(TEMPLATE), environ={})
        self.assertEqual(settings.storage.type, "sqlite")
        self.assertEqual(set(settings.storages), {"sqlite", "postgres", "mysql", "sqlserver", "redis", "memory"})
        self.assertEqual(settings.server.queues, ["critical", "default"])
        self.assertEqual(settings.jobs.expiration, 86400)
        self.assertTrue(settings.dashboard.enabled)

    def test_full_file(self):
        path = self.write('''
[storage]
use = "main"

[storage.main]
type = "postgresql"
host = "${PG_HOST:-db.local}"
user = "app"
password = "${PG_PASSWORD}"
database = "jobs"
schema = "bg"
job_expiration = "2d"

[storage.cache]
type = "redis"
url = "redis://cache:6379/1"

[jobs]
expiration = "12h"
invisibility_timeout = "10m"

[retry]
attempts = 3
delays = ["5s", "1m"]
on_attempts_exceeded = "delete"
except_on = ["builtins.ValueError"]

[server]
name = "worker-a"
workers = 7
queues = "critical, default"
imports = ["tests.jobs"]
shutdown_timeout = "30s"

[dashboard]
port = 9000
username = "admin"
password = "${DASH_PW:-changeme}"
read_only = true

[logging]
level = "debug"

[[recurring]]
id = "sum"
job = "tests.jobs:add"
cron = "@hourly"
args = [1, 2]
''')
        s = load_config(path, environ={"PG_PASSWORD": "s3cr3t"})
        self.assertEqual((s.storage.name, s.storage.type), ("main", "postgres"))
        self.assertEqual(s.storage.options["host"], "db.local")
        self.assertEqual(s.storage.options["password"], "s3cr3t")
        self.assertEqual(s.storage.options["job_expiration"], 172800)
        self.assertEqual((s.jobs.expiration, s.jobs.invisibility_timeout), (43200, 600))
        self.assertEqual((s.retry.attempts, s.retry.delays, s.retry.on_attempts_exceeded), (3, [5, 60], "delete"))
        self.assertEqual((s.server.name, s.server.workers, s.server.queues), ("worker-a", 7, ["critical", "default"]))
        self.assertEqual(s.server.shutdown_timeout, 30)
        self.assertEqual((s.dashboard.port, s.dashboard.password, s.dashboard.read_only), (9000, "changeme", True))
        self.assertEqual(s.log_level, "DEBUG")
        self.assertEqual(s.recurring[0].args, [1, 2])
        described = s.describe()
        self.assertNotIn("s3cr3t", described)
        self.assertIn("main (type postgres)", described)
        self.assertIn("also defined: cache", described)
        retry = s.retry.create_filter()
        self.assertIs(retry.except_on[0], ValueError)

    def test_environment_selects_storage(self):
        path = self.write('''
[storage]
use = "a"
[storage.a]
type = "memory"
[storage.b]
type = "sqlite"
path = "b.db"
''')
        self.assertEqual(load_config(path, environ={}).storage.name, "a")
        self.assertEqual(load_config(path, environ={"SCHEDULEFIRE_STORAGE": "b"}).storage.name, "b")
        via_url = load_config(path, environ={"SCHEDULEFIRE_DB": "redis://x:6379/0"}).storage
        self.assertEqual((via_url.type, via_url.options), ("redis", {"url": "redis://x:6379/0"}))

    def test_errors_are_helpful(self):
        cases = {
            "[server]\nwokers = 3\n": 'did you mean "workers"',
            "[storage]\nuse = \"pg\"\n[storage.sqlite]\n": "no [storage.pg] section",
            "[storage.a]\ntype = \"memory\"\n[storage.b]\ntype = \"memory\"\n": "choose one with use",
            "[storage.x]\ntype = \"oracle\"\n": "unknown type",
            "[storage.pg]\ntype = \"postgres\"\nhots = \"x\"\n": 'did you mean "host"',
            "[server]\nqueues = [\"Bad Name\"]\n": "server.queues",
            "[server]\nworkers = \"ten\"\n": "expected an integer",
            "[dashboard]\nusername = \"admin\"\n": "password",
            "[[recurring]]\nid = \"x\"\njob = \"m:f\"\ncron = \"not cron\"\n": "recurring[0].cron",
            "[storage.pg]\ntype = \"postgres\"\npassword = \"${NOPE}\"\n": "NOPE is not set",
            "[unknown]\n": 'unknown setting "unknown"',
        }
        for text, message in cases.items():
            with self.subTest(text=text):
                with self.assertRaises(ConfigError) as ctx:
                    load_config(self.write(text), environ={})
                self.assertIn(message, str(ctx.exception))
                self.assertIn("schedulefire.toml", str(ctx.exception))

    def test_connection_fields_build_urls(self):
        self.assertEqual(_postgres_dsn({"host": "h", "user": "u", "password": "p@ss", "database": "d",
                                        "options": {"sslmode": "require"}}),
                         "postgresql://u:p%40ss@h:5432/d?sslmode=require")
        self.assertEqual(_mysql_url({"host": "h", "port": 3307, "user": "u", "database": "d"}),
                         "mysql://u@h:3307/d")
        self.assertEqual(_redis_url({"host": "h", "password": "pw", "db": 2, "ssl": True}), "rediss://:pw@h:6379/2")
        self.assertEqual(_sqlserver_connection({"host": "sql", "port": 1433, "database": "d", "user": "sa",
                                                "password": "x;y", "trust_server_certificate": True}),
                         "mssql://sa:x%3By@sql:1433/d?TrustServerCertificate=yes")
        self.assertEqual(_sqlserver_connection({"host": "sql", "database": "d"}), "mssql://sql/d")

    def test_mask_secrets(self):
        self.assertEqual(mask_secrets("postgresql://app:hunter2@db/x"), "postgresql://app:****@db/x")
        self.assertEqual(mask_secrets("Driver={X};UID=sa;PWD={a;b};Server=s"), "Driver={X};UID=sa;PWD=****;Server=s")

    def test_json_config(self):
        path = self.write(json.dumps({"storage": {"use": "mem", "mem": {"type": "memory"}},
                                      "server": {"workers": 2}}), name="schedulefire.json")
        s = load_config(path, environ={})
        self.assertIsInstance(s.create_storage(), MemoryStorage)
        self.assertEqual(s.server.workers, 2)

    def test_no_file_defaults(self):
        s = parse_settings({}, environ={})
        self.assertEqual((s.storage.type, s.storage.options), ("sqlite", {"path": "schedulefire.db"}))


class ApplyTests(ConfigFileCase):
    def setUp(self):
        super().setUp()
        saved_storage = config._storage
        saved_filters = list(GlobalJobFilters)
        self.addCleanup(self._restore, saved_storage, saved_filters)

    @staticmethod
    def _restore(storage, filters):
        if config._storage is not None and config._storage is not storage:
            config._storage.close()
        config._storage = storage
        GlobalJobFilters.clear()
        for f in filters:
            GlobalJobFilters.add(f)

    def test_apply_configures_everything(self):
        db = os.path.join(self.tmp.name, "sub", "jobs.db").replace("\\", "/")
        path = self.write(f'''
[storage.local]
type = "sqlite"
path = "{db}"
[retry]
attempts = 2
[server]
workers = 3
queues = ["critical"]
schedule_polling_interval = "2s"
[[recurring]]
id = "sum"
job = "tests.jobs:add"
cron = "*/5 * * * *"
args = [4, 5]
''')
        settings = load_config(path, environ={})
        storage = settings.apply(register_recurring=True)
        self.assertIsInstance(storage, SQLiteStorage)
        self.assertIs(config.get_storage(), storage)
        retries = [f for f in GlobalJobFilters if isinstance(f, AutomaticRetry)]
        self.assertEqual([f.attempts for f in retries], [2])
        rec = storage.get_recurring("sum")
        self.assertEqual((rec.cron, rec.invocation["args"], rec.invocation["qualname"]), ("*/5 * * * *", [4, 5], "add"))
        server = settings.create_server(storage)
        self.assertEqual((server.worker_count, server.queues, server.schedule_polling_interval), (3, ["critical"], 2))

    def test_get_storage_discovers_config_file(self):
        self.write('[storage.mem]\ntype = "memory"\n[retry]\nattempts = 4\n')
        config._storage = None
        cwd = os.getcwd()
        os.chdir(self.tmp.name)
        try:
            os.environ.pop("SCHEDULEFIRE_CONFIG", None)
            self.assertIsInstance(config.get_storage(), MemoryStorage)
            self.assertEqual([f.attempts for f in GlobalJobFilters if isinstance(f, AutomaticRetry)], [4])
        finally:
            os.chdir(cwd)


class CliTests(ConfigFileCase):
    def run_cli(self, *args, env=None):
        environment = dict(os.environ, PYTHONPATH=ROOT, **(env or {}))
        environment.pop("SCHEDULEFIRE_DB", None)
        environment.pop("SCHEDULEFIRE_CONFIG", None)
        return subprocess.run([sys.executable, "-m", "schedulefire", *args], cwd=self.tmp.name,
                              capture_output=True, text=True, env=environment, timeout=60)

    def test_init_and_check(self):
        result = self.run_cli("config", "init")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(os.path.exists(os.path.join(self.tmp.name, "schedulefire.toml")))
        self.assertNotEqual(self.run_cli("config", "init").returncode, 0)  # refuses to overwrite

        check = self.run_cli("config", "check")
        self.assertEqual(check.returncode, 0, check.stderr)
        self.assertIn("storage connection OK", check.stdout)
        self.assertTrue(os.path.exists(os.path.join(self.tmp.name, "data", "schedulefire.db")))

        other = self.run_cli("config", "check", "--storage", "memory")
        self.assertIn("memory (type memory)", other.stdout)

    def test_check_reports_errors(self):
        self.write("[server]\nwokers = 3\n")
        result = self.run_cli("config", "check")
        self.assertEqual(result.returncode, 2)
        self.assertIn('did you mean "workers"', result.stderr)

        self.write('[storage.pg]\ntype = "postgres"\nhost = "127.0.0.1"\nport = 1\ndatabase = "x"\n')
        failed = self.run_cli("config", "check")
        self.assertEqual(failed.returncode, 1)
        self.assertIn("storage connection FAILED", failed.stderr)


if __name__ == "__main__":
    unittest.main()

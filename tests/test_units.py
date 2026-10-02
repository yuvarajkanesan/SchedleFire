import datetime as dt
import decimal
import os
import tempfile
import unittest
import uuid

from schedulefire import BackgroundJob, Cron, CronExpression, Job, MemoryStorage, SQLiteStorage
from schedulefire import _serialization
from tests import jobs
from tests._compat import TemporaryDirectory

UTC = dt.timezone.utc


def at(*args):
    return dt.datetime(*args, tzinfo=UTC)


class CronTests(unittest.TestCase):
    def next(self, expr, after):
        return CronExpression(expr).next_occurrence(after)

    def test_every_minute(self):
        self.assertEqual(self.next("* * * * *", at(2026, 1, 1, 10, 0, 30)), at(2026, 1, 1, 10, 1))

    def test_strictly_after(self):
        self.assertEqual(self.next("0 9 * * *", at(2026, 1, 1, 9, 0)), at(2026, 1, 2, 9, 0))

    def test_steps_ranges_lists(self):
        e = CronExpression("*/15 9-17 * * 1-5")
        self.assertEqual(e.minutes, {0, 15, 30, 45})
        self.assertEqual(e.hours, set(range(9, 18)))
        self.assertEqual(e.weekdays, {1, 2, 3, 4, 5})
        # Friday 2026-01-02 17:50 -> Monday 09:00
        self.assertEqual(e.next_occurrence(at(2026, 1, 2, 17, 50)), at(2026, 1, 5, 9, 0))

    def test_names_and_sunday_7(self):
        self.assertEqual(CronExpression("0 0 * jan,dec sun").months, {1, 12})
        self.assertEqual(CronExpression("0 0 * * 7").weekdays, {0})
        self.assertEqual(CronExpression("0 0 * * mon-fri").weekdays, {1, 2, 3, 4, 5})

    def test_dom_or_dow(self):
        # 1st of month OR any Monday
        e = CronExpression("0 0 1 * mon")
        self.assertEqual(e.next_occurrence(at(2026, 1, 1, 0, 0)), at(2026, 1, 5))

    def test_month_rollover_and_leap_day(self):
        self.assertEqual(self.next("0 0 29 2 *", at(2026, 3, 1)), at(2028, 2, 29))
        self.assertEqual(self.next("30 23 31 * *", at(2026, 4, 1)), at(2026, 5, 31, 23, 30))

    def test_macros(self):
        self.assertEqual(self.next("@daily", at(2026, 1, 1, 5)), at(2026, 1, 2))
        self.assertEqual(self.next("@hourly", at(2026, 1, 1, 5, 10)), at(2026, 1, 1, 6))

    def test_never(self):
        self.assertIsNone(self.next(Cron.never(), at(2026, 1, 1)))

    def test_invalid(self):
        for bad in ["", "* * * *", "60 * * * *", "* 24 * * *", "*/0 * * * *", "5-1 * * * *", "x * * * *"]:
            with self.assertRaises(ValueError, msg=bad):
                CronExpression(bad)

    def test_helpers(self):
        self.assertEqual(Cron.daily(9, 30), "30 9 * * *")
        self.assertEqual(Cron.weekly("fri", 17), "0 17 * * 5")
        self.assertEqual(Cron.monthly(15), "0 0 15 * *")
        for expr in [Cron.minutely(), Cron.hourly(5), Cron.yearly(6, 1), Cron.minute_interval(10)]:
            CronExpression(expr)

    def test_time_zone(self):
        tz = dt.timezone(dt.timedelta(hours=5, minutes=30))  # fixed offset, no tzdata needed
        nxt = CronExpression("0 9 * * *").next_occurrence(at(2026, 1, 1, 0, 0), tz)
        self.assertEqual(nxt, at(2026, 1, 1, 3, 30))


class SerializationTests(unittest.TestCase):
    def test_round_trip(self):
        value = {
            "dt": dt.datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
            "d": dt.date(2026, 1, 2),
            "td": dt.timedelta(minutes=5),
            "dec": decimal.Decimal("1.10"),
            "id": uuid.UUID(int=5),
            "b": b"\x00\xff",
            "s": {1, 2},
            "nested": [1, "x", None, True, {"k": 1.5}],
        }
        self.assertEqual(_serialization.loads(_serialization.dumps(value)), value)

    def test_rejects_objects(self):
        with self.assertRaises(TypeError):
            _serialization.dumps(object())


class JobTests(unittest.TestCase):
    def test_function(self):
        job = Job(jobs.add, (1, 2))
        self.assertEqual((job.module, job.qualname, job.queue), ("tests.jobs", "add", "default"))
        self.assertEqual(Job.from_invocation(job.to_invocation()).invoke(), 3)

    def test_queue_decorator_and_override(self):
        self.assertEqual(Job(jobs.critical_task, (1,)).queue, "critical")
        self.assertEqual(Job(jobs.critical_task, (1,), queue="low").queue, "low")
        with self.assertRaises(ValueError):
            Job(jobs.add, (1, 2), queue="Bad Name")

    def test_methods(self):
        self.assertEqual(Job(jobs.Greeter.greet, ("a",)).qualname, "Greeter.greet")
        self.assertEqual(Job(jobs.Greeter.static_greet, ("a",)).qualname, "Greeter.static_greet")
        self.assertEqual(Job(jobs.Greeter.class_greet, ("a",)).qualname, "Greeter.class_greet")
        self.assertEqual(Job(jobs.Greeter.greet, ("bob",)).invoke(), "hello bob")

    def test_rejects_unserializable_targets(self):
        with self.assertRaises(TypeError):
            Job(lambda: None)
        with self.assertRaises(TypeError):
            Job(jobs.Greeter().greet, ("a",))

        def nested():
            pass

        with self.assertRaises(TypeError):
            Job(nested)
        with self.assertRaises(TypeError):
            Job(jobs.add, (object(), 1))


class ConnectionUrlTests(unittest.TestCase):
    def test_mysql_url(self):
        from schedulefire.storage.mysql import _parse_url

        params = _parse_url("mysql://app:p%40ss@db.example.com:3307/jobs?connect_timeout=5")
        self.assertEqual(params, {"host": "db.example.com", "port": 3307, "user": "app", "password": "p@ss",
                                  "database": "jobs", "connect_timeout": 5})
        with self.assertRaises(ValueError):
            _parse_url("mysql://app@db/")

    def test_sqlserver_url(self):
        from schedulefire.storage.sqlserver import connection_string_from_url

        class FakeOdbc:
            @staticmethod
            def drivers():
                return ["SQL Server", "ODBC Driver 17 for SQL Server", "ODBC Driver 18 for SQL Server"]

        cs = connection_string_from_url("mssql://sa:p%3Bw%7D@sql01:1433/jobs?TrustServerCertificate=yes", FakeOdbc)
        self.assertEqual(cs, "Driver={ODBC Driver 18 for SQL Server};Server=sql01,1433;Database=jobs;"
                             "UID=sa;PWD={p;w}}};TrustServerCertificate=yes;")
        trusted = connection_string_from_url("sqlserver://sql01/jobs", FakeOdbc)
        self.assertIn("Trusted_Connection=yes", trusted)
        self.assertNotIn("UID=", trusted)


class DefaultStorageTests(unittest.TestCase):
    def setUp(self):
        from schedulefire import config

        self.config = config
        self.saved = (config._storage, os.environ.pop("SCHEDULEFIRE_DB", None))
        config._storage = None
        self.tmp = TemporaryDirectory()

    def tearDown(self):
        if self.config._storage is not None:
            self.config._storage.close()
        self.config._storage, env = self.saved
        if env is not None:
            os.environ["SCHEDULEFIRE_DB"] = env
        else:
            os.environ.pop("SCHEDULEFIRE_DB", None)
        self.tmp.cleanup()

    def test_falls_back_to_sqlite_file(self):
        cwd = os.getcwd()
        os.chdir(self.tmp.name)
        try:
            with self.assertLogs("schedulefire", "WARNING"):
                storage = self.config.get_storage()
            self.assertIsInstance(storage, SQLiteStorage)
            self.assertTrue(os.path.exists(os.path.join(self.tmp.name, "schedulefire.db")))
            self.assertIs(self.config.get_storage(), storage)  # created once
            job_id = BackgroundJob.enqueue(jobs.add, 1, 2)
            self.assertEqual(storage.get_job(job_id).state_name, "Enqueued")
        finally:
            self.config._storage.close()
            os.chdir(cwd)

    def test_environment_variable(self):
        os.environ["SCHEDULEFIRE_DB"] = os.path.join(self.tmp.name, "env.db")
        storage = self.config.get_storage()
        self.assertIsInstance(storage, SQLiteStorage)
        self.assertEqual(storage.path, os.environ["SCHEDULEFIRE_DB"])
        os.environ["SCHEDULEFIRE_DB"] = "memory://"
        self.config._storage.close()
        self.config._storage = None
        self.assertIsInstance(self.config.get_storage(), MemoryStorage)


if __name__ == "__main__":
    unittest.main()

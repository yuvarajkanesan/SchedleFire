"""Editing recurring jobs and rescheduling scheduled jobs from the dashboard."""

import io
import time
import unittest
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote

from schedulefire import BackgroundJobClient, Cron, MemoryStorage, RecurringJobManager
from schedulefire.dashboard import DashboardApp
from tests import jobs


class Browser:
    """Calls the WSGI app like a browser: keeps cookies between requests."""

    def __init__(self, app, host="dash.test", remote="10.1.1.1"):
        self.app, self.host, self.remote = app, host, remote
        self.cookies = {}

    def request(self, url, method="GET", body="", **headers):
        path, _, query = url.partition("?")
        environ = {"REQUEST_METHOD": method, "PATH_INFO": path, "QUERY_STRING": query, "SCRIPT_NAME": "",
                   "REMOTE_ADDR": self.remote, "HTTP_HOST": self.host, "SERVER_NAME": self.host, "SERVER_PORT": "80",
                   "wsgi.url_scheme": "http", "CONTENT_LENGTH": str(len(body)), "wsgi.input": io.BytesIO(body.encode()),
                   "HTTP_COOKIE": "; ".join(f"{k}={v}" for k, v in self.cookies.items())}
        environ.update(headers)
        out = {}
        chunks = self.app(environ, lambda status, hdrs: out.update(status=status, headers=hdrs))
        for name, value in out["headers"]:
            if name == "Set-Cookie":
                pair = value.split(";")[0]
                key, _, val = pair.partition("=")
                if "Max-Age=0" in value:
                    self.cookies.pop(key, None)
                else:
                    self.cookies[key] = val
        out["body"] = b"".join(chunks).decode("utf-8", "replace")
        out["location"] = dict(out["headers"]).get("Location", "")
        return out

    def csrf(self, page="/"):
        self.request(page)
        return self.cookies["schedulefire_csrf"]

    def post(self, path, data, **headers):
        headers.setdefault("HTTP_ORIGIN", f"http://{self.host}")
        res = self.request(path, "POST", data, **headers)
        res["location"] = unquote(res["location"])  # so assertions can match plain text, not %20 etc.
        return res


# ------------------------------------------------------------------ editing recurring jobs


class RecurringEditTests(unittest.TestCase):
    def setUp(self):
        self.storage = MemoryStorage()
        RecurringJobManager(self.storage).add_or_update("nightly", jobs.add, Cron.daily(2), queue="default")
        self.app = DashboardApp(self.storage)

    def test_page_shows_an_edit_form_per_job(self):
        page = Browser(self.app).request("/recurring")
        self.assertIn('name="cron"', page["body"])
        self.assertIn("data-open-edit", page["body"])

    def test_updates_cron_time_zone_and_queue_keeping_the_target(self):
        b = Browser(self.app)
        token = b.csrf()
        res = b.post("/actions/recurring/update", f"id=nightly&cron=0+3+*+*+*&time_zone=UTC&queue=low&csrf={token}&next=/recurring")
        self.assertEqual(res["status"], "303 See Other")
        record = self.storage.get_recurring("nightly")
        self.assertEqual((record.cron, record.queue, record.job.qualname), ("0 3 * * *", "low", "add"))

    def test_invalid_cron_is_rejected_without_changing_anything(self):
        b = Browser(self.app)
        token = b.csrf()
        res = b.post("/actions/recurring/update", f"id=nightly&cron=nonsense&time_zone=UTC&queue=default&csrf={token}&next=/recurring")
        self.assertIn("level=error", res["location"])
        self.assertEqual(self.storage.get_recurring("nightly").cron, Cron.daily(2))

    def test_identical_values_are_a_no_op(self):
        b = Browser(self.app)
        token = b.csrf()
        cron = Cron.daily(2).replace(" ", "+")
        res = b.post("/actions/recurring/update", f"id=nightly&cron={cron}&time_zone=UTC&queue=default&csrf={token}&next=/recurring")
        self.assertIn("No changes", res["location"])

    def test_unknown_recurring_id(self):
        b = Browser(self.app)
        token = b.csrf()
        res = b.post("/actions/recurring/update", f"id=ghost&cron=*+*+*+*+*&time_zone=UTC&queue=default&csrf={token}&next=/recurring")
        self.assertIn("no longer exists", res["location"])

    def test_read_only_hides_edit_ui_and_blocks_the_post(self):
        app = DashboardApp(self.storage, read_only=True)
        b = Browser(app)
        page = b.request("/recurring")
        self.assertNotIn("data-open-edit", page["body"])
        self.assertNotIn("/actions/recurring/update", page["body"])
        res = b.request("/actions/recurring/update", "POST", "id=nightly&cron=*+*+*+*+*")
        self.assertEqual(res["status"], "403 Forbidden")

    def test_friendly_daily_builder(self):
        b = Browser(self.app)
        token = b.csrf()
        res = b.post("/actions/recurring/update",
                     f"id=nightly&frequency=daily&time_daily=14:30&time_zone=UTC&queue=default&csrf={token}&next=/recurring")
        self.assertEqual(res["status"], "303 See Other")
        self.assertEqual(self.storage.get_recurring("nightly").cron, "30 14 * * *")

    def test_friendly_weekly_builder(self):
        b = Browser(self.app)
        token = b.csrf()
        b.post("/actions/recurring/update",
              f"id=nightly&frequency=weekly&day_of_week=3&time_weekly=08:05&time_zone=UTC&queue=default&csrf={token}&next=/recurring")
        self.assertEqual(self.storage.get_recurring("nightly").cron, "5 8 * * 3")

    def test_friendly_monthly_builder(self):
        b = Browser(self.app)
        token = b.csrf()
        b.post("/actions/recurring/update",
              f"id=nightly&frequency=monthly&day_of_month=15&time_monthly=00:00&time_zone=UTC&queue=default&csrf={token}&next=/recurring")
        self.assertEqual(self.storage.get_recurring("nightly").cron, "0 0 15 * *")

    def test_friendly_minutes_and_hourly_builders(self):
        b = Browser(self.app)
        token = b.csrf()
        b.post("/actions/recurring/update",
              f"id=nightly&frequency=minutes&interval_minutes=15&time_zone=UTC&queue=default&csrf={token}&next=/recurring")
        self.assertEqual(self.storage.get_recurring("nightly").cron, "*/15 * * * *")
        b.post("/actions/recurring/update",
              f"id=nightly&frequency=hourly&interval_hours=3&minute=20&time_zone=UTC&queue=default&csrf={token}&next=/recurring")
        self.assertEqual(self.storage.get_recurring("nightly").cron, "20 */3 * * *")

    def test_friendly_builder_validation_errors(self):
        b = Browser(self.app)
        token = b.csrf()
        cases = [
            "frequency=daily&time_daily=nonsense",
            "frequency=weekly&day_of_week=9&time_weekly=08:00",
            "frequency=monthly&day_of_month=99&time_monthly=08:00",
            "frequency=minutes&interval_minutes=0",
            "frequency=hourly&interval_hours=99&minute=0",
        ]
        for case in cases:
            res = b.post("/actions/recurring/update", f"id=nightly&{case}&time_zone=UTC&queue=default&csrf={token}&next=/recurring")
            self.assertIn("level=error", res["location"], case)
        self.assertEqual(self.storage.get_recurring("nightly").cron, Cron.daily(2))  # unchanged

    def test_page_prefills_the_detected_frequency(self):
        RecurringJobManager(self.storage).add_or_update("weekly-report", jobs.add, "30 9 * * 2", queue="default")
        page = Browser(self.app).request("/recurring")["body"]
        self.assertIn('<option value="weekly" selected>', page)
        self.assertIn('value="09:30"', page)
        self.assertIn('<option value="2" selected>Tuesday</option>', page)

    def test_page_includes_time_zone_and_queue_suggestions(self):
        page = Browser(self.app).request("/recurring")["body"]
        self.assertIn("America/New_York", page)  # a common-zones datalist suggestion
        self.assertIn('<datalist id="q-nightly">', page)


# ------------------------------------------------------------------ rescheduling scheduled jobs


class RescheduleTests(unittest.TestCase):
    def setUp(self):
        self.storage = MemoryStorage()
        self.client = BackgroundJobClient(self.storage)
        self.app = DashboardApp(self.storage)

    def test_job_details_page_offers_reschedule_only_while_scheduled(self):
        scheduled = self.client.schedule(jobs.add, 3600, 1, 1)
        enqueued = self.client.enqueue(jobs.add, 1, 1)
        b = Browser(self.app)
        self.assertIn("/actions/jobs/reschedule", b.request(f"/job/{scheduled}")["body"])
        self.assertNotIn("/actions/jobs/reschedule", b.request(f"/job/{enqueued}")["body"])

    def test_reschedule_moves_a_scheduled_job(self):
        job_id = self.client.schedule(jobs.add, 3600, 1, 1)
        before = self.storage.get_job(job_id).state_data["enqueue_at"]
        b = Browser(self.app)
        token = b.csrf()
        res = b.post("/actions/jobs/reschedule", f"id={job_id}&amount=5&unit=minutes&csrf={token}&next=/job/{job_id}")
        self.assertEqual(res["status"], "303 See Other")
        after = self.storage.get_job(job_id).state_data["enqueue_at"]
        self.assertNotEqual(before, after)
        self.assertEqual(self.storage.get_job(job_id).state_name, "Scheduled")

    def test_unit_conversion(self):
        for unit, factor in (("minutes", 60), ("hours", 3600), ("days", 86400)):
            job_id = self.client.schedule(jobs.add, 3600, 1, 1)
            b = Browser(self.app)
            token = b.csrf()
            t0 = time.time()
            b.post("/actions/jobs/reschedule", f"id={job_id}&amount=2&unit={unit}&csrf={token}&next=/job/{job_id}")
            from schedulefire.dashboard.components import parse_iso

            when = parse_iso(self.storage.get_job(job_id).state_data["enqueue_at"])
            self.assertAlmostEqual(when - t0, 2 * factor, delta=5)

    def test_rejects_a_job_that_is_no_longer_scheduled(self):
        job_id = self.client.enqueue(jobs.add, 1, 1)  # already Enqueued, not Scheduled
        b = Browser(self.app)
        token = b.csrf()
        res = b.post("/actions/jobs/reschedule", f"id={job_id}&amount=5&unit=minutes&csrf={token}&next=/job/{job_id}")
        self.assertIn("no longer Scheduled", res["location"])

    def test_rejects_non_positive_or_non_numeric_amount(self):
        job_id = self.client.schedule(jobs.add, 3600, 1, 1)
        b = Browser(self.app)
        token = b.csrf()
        for amount in ("0", "-5", "not-a-number", ""):
            res = b.post("/actions/jobs/reschedule", f"id={job_id}&amount={amount}&unit=minutes&csrf={token}&next=/job/{job_id}")
            self.assertIn("level=error", res["location"], amount)

    def test_read_only_hides_form_and_blocks_post(self):
        job_id = BackgroundJobClient(self.storage).schedule(jobs.add, 3600, 1, 1)
        app = DashboardApp(self.storage, read_only=True)
        b = Browser(app)
        page = b.request(f"/job/{job_id}")
        self.assertNotIn("/actions/jobs/reschedule", page["body"])
        res = b.request("/actions/jobs/reschedule", "POST", f"id={job_id}&amount=5&unit=minutes")
        self.assertEqual(res["status"], "403 Forbidden")

    def test_reschedule_at_a_specific_time(self):
        from schedulefire.dashboard.components import parse_iso

        job_id = self.client.schedule(jobs.add, 3600, 1, 1)
        b = Browser(self.app)
        token = b.csrf()
        target = (datetime.now(timezone.utc) + timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M")
        res = b.post("/actions/jobs/reschedule",
                     f"id={job_id}&when_mode=at&at={target}&csrf={token}&next=/job/{job_id}")
        self.assertEqual(res["status"], "303 See Other")
        when = parse_iso(self.storage.get_job(job_id).state_data["enqueue_at"])
        expected = datetime.strptime(target, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc).timestamp()
        self.assertAlmostEqual(when, expected, delta=2)

    def test_reschedule_at_rejects_a_past_time(self):
        job_id = self.client.schedule(jobs.add, 3600, 1, 1)
        b = Browser(self.app)
        token = b.csrf()
        past = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M")
        res = b.post("/actions/jobs/reschedule", f"id={job_id}&when_mode=at&at={past}&csrf={token}&next=/job/{job_id}")
        self.assertIn("level=error", res["location"])

    def test_reschedule_at_rejects_garbage_input(self):
        job_id = self.client.schedule(jobs.add, 3600, 1, 1)
        b = Browser(self.app)
        token = b.csrf()
        res = b.post("/actions/jobs/reschedule",
                     f"id={job_id}&when_mode=at&at=not-a-date&csrf={token}&next=/job/{job_id}")
        self.assertIn("level=error", res["location"])


if __name__ == "__main__":
    unittest.main()

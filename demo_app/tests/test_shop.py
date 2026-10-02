"""End-to-end tests for the demo shop, using scheduleFire as an INSTALLED package.

A real worker process (the `schedulefire` command from this virtual environment) runs the
jobs; the Flask app is driven through its test client, exactly like a browser would.

Run from demo_app/:   .venv\\Scripts\\python -m unittest -v
"""

import base64
import importlib
import os
import shutil
import subprocess
import sys
import sysconfig
import tempfile
import time
import unittest

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TMP = tempfile.mkdtemp(prefix="shop-e2e-")
ADMIN = {"Authorization": "Basic " + base64.b64encode(b"admin:test-pass-123").decode()}

# Point both the app and the worker at throwaway databases; everything else comes from schedulefire.toml.
os.environ.update(
    SCHEDULEFIRE_DB=os.path.join(TMP, "jobs.db"),
    SHOP_DB=os.path.join(TMP, "shop.db"),
    SHOP_ADMIN_PASSWORD="test-pass-123",
    SHOP_REVIEW_DELAY="2",
)
os.chdir(HERE)


def wait_until(predicate, timeout=30.0, interval=0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return None


class ShopEndToEnd(unittest.TestCase):
    worker = None

    @classmethod
    def setUpClass(cls):
        cls.web = importlib.import_module("shop.web")
        cls.client = cls.web.app.test_client()
        cls.storage = cls.web.get_storage()
        exe = os.path.join(os.path.dirname(sys.executable), "schedulefire" + (".exe" if os.name == "nt" else ""))
        cls.log = open(os.path.join(TMP, "worker.log"), "w")
        cls.worker = subprocess.Popen([exe, "server", "--schedule-interval", "0.5"], cwd=HERE,
                                      stdout=cls.log, stderr=subprocess.STDOUT)
        if not wait_until(lambda: cls.storage.get_servers(), timeout=30):
            raise RuntimeError("worker did not start; see " + cls.log.name)

    @classmethod
    def tearDownClass(cls):
        if cls.worker:
            cls.worker.terminate()
            cls.worker.wait(30)
        cls.log.close()
        cls.storage.close()
        shutil.rmtree(TMP, ignore_errors=True)

    # -- helpers ------------------------------------------------------------------

    def order(self, email, amount=10.0):
        res = self.client.post("/orders", json={"email": email, "amount": amount})
        self.assertEqual(res.status_code, 202)
        return res.get_json()

    def state(self, job_id):
        record = self.storage.get_job(job_id)
        return record.state_name if record else None

    def wait_state(self, job_id, *states, timeout=30):
        ok = wait_until(lambda: self.state(job_id) in states, timeout=timeout)
        self.assertTrue(ok, f"job #{job_id} is {self.state(job_id)}, expected {states}")

    def csrf(self, page="/schedule-fire/"):
        self.assertEqual(self.client.get(page, headers=ADMIN).status_code, 200)
        return self.client.get_cookie("schedulefire_csrf", path="/schedule-fire").value

    # -- 1. packaging ----------------------------------------------------------------

    def test_01_uses_the_installed_wheel(self):
        import schedulefire

        site = os.path.normcase(sysconfig.get_paths()["purelib"])
        self.assertTrue(os.path.normcase(schedulefire.__file__).startswith(site), schedulefire.__file__)
        self.assertEqual(schedulefire.__version__, "0.1.0")

    def test_02_config_check(self):
        exe = os.path.join(os.path.dirname(sys.executable), "schedulefire")
        out = subprocess.run([exe, "config", "check"], cwd=HERE, capture_output=True, text=True, timeout=60)
        self.assertEqual(out.returncode, 0, out.stderr)
        self.assertIn("jobs limited to shop.jobs", out.stdout)
        self.assertIn("auth basic", out.stdout)
        self.assertIn("storage connection OK", out.stdout)

    # -- 2. job types ----------------------------------------------------------------

    def test_03_order_runs_every_job_type(self):
        placed = self.order("ada@example.com", 49.9)
        jobs = placed["jobs"]
        self.assertEqual(self.state(jobs["invoice"]), "Awaiting")      # continuation waits for the charge
        self.assertEqual(self.state(jobs["review"]), "Scheduled")      # delayed by SHOP_REVIEW_DELAY
        for key in ("confirmation", "charge", "invoice"):
            self.wait_state(jobs[key], "Succeeded")
        self.wait_state(jobs["review"], "Succeeded", timeout=20)
        order = self.client.get(f"/orders/{placed['order_id']}").get_json()
        self.assertEqual((order["status"], order["confirmation_sent"], order["review_requested"]), ("invoiced", 1, 1))
        self.assertEqual(order["invoice"], f"INV-{placed['order_id']:05d}.pdf")
        # queues come from @queue decorators
        self.assertEqual(self.storage.get_job(jobs["charge"]).queue, "critical")
        self.assertEqual(self.storage.get_job(jobs["review"]).queue, "low")
        self.assertEqual(self.storage.get_job(jobs["confirmation"]).queue, "default")

    def test_04_retries_then_succeeds(self):
        placed = self.order("flaky@example.com")
        charge = placed["jobs"]["charge"]
        self.wait_state(charge, "Succeeded", timeout=40)
        history = [s.name for s in self.storage.get_state_history(charge)]
        self.assertEqual(history.count("Scheduled"), 2, history)        # two retries
        self.assertEqual(self.storage.get_job(charge).parameters["RetryCount"], 2)
        self.wait_state(placed["jobs"]["invoice"], "Succeeded")
        self.assertEqual(self.client.get(f"/orders/{placed['order_id']}").get_json()["payment_attempts"], 3)

    def test_05_declined_payment_is_not_retried(self):
        placed = self.order("declined@example.com")
        charge = placed["jobs"]["charge"]
        self.wait_state(charge, "Failed")
        record = self.storage.get_job(charge)
        self.assertTrue(record.state_data["exception_type"].endswith("PaymentDeclined"))
        self.assertNotIn("Scheduled", [s.name for s in self.storage.get_state_history(charge)])
        time.sleep(1.5)
        self.assertEqual(self.state(placed["jobs"]["invoice"]), "Awaiting")   # no invoice for a failed payment

    def test_06_recurring_jobs_from_config(self):
        recurring = {r.id: r for r in self.storage.get_recurring_jobs()}
        self.assertEqual(set(recurring), {"nightly-sales-report", "cleanup-abandoned-carts"})
        self.assertEqual(recurring["nightly-sales-report"].cron, "0 2 * * *")
        self.assertIsNotNone(recurring["cleanup-abandoned-carts"].next_execution)
        token = self.csrf("/schedule-fire/schedules")
        res = self.client.post("/schedule-fire/actions/recurring/trigger", headers=ADMIN,
                               data={"id": "nightly-sales-report", "next": "/recurring", "csrf": token})
        self.assertEqual(res.status_code, 303)
        self.assertTrue(res.headers["Location"].startswith("/schedule-fire/schedules?notice="))
        job_id = self.storage.get_recurring("nightly-sales-report").last_job_id
        self.wait_state(job_id, "Succeeded")
        self.assertIn("revenue", self.storage.get_job(job_id).state_data["result"])

    # -- 3. dashboard ---------------------------------------------------------------

    def test_07_dashboard_login_routes_and_custom_page(self):
        self.order("dash@example.com")
        self.assertEqual(self.client.get("/schedule-fire/").status_code, 302)             # -> sign-in page
        bad = {"Authorization": "Basic " + base64.b64encode(b"admin:wrong").decode()}
        self.assertEqual(self.client.get("/schedule-fire/", headers=bad).status_code, 401)
        home = self.client.get("/schedule-fire/", headers=ADMIN)
        self.assertEqual(home.status_code, 200)
        self.assertIn(b'href="/schedule-fire/schedules"', home.data)                      # custom route
        self.assertEqual(self.client.get("/schedule-fire/recurring", headers=ADMIN).status_code, 404)
        shop = self.client.get("/schedule-fire/shop", headers=ADMIN)                        # custom page
        self.assertEqual(shop.status_code, 200)
        self.assertIn(b"dash@example.com", shop.data)
        self.assertIn(b"shop-worker", self.client.get("/schedule-fire/servers", headers=ADMIN).data)
        # the app's own routes are untouched, including one called /jobs
        self.assertIn(b"Demo shop", self.client.get("/").data)
        self.assertEqual(self.client.get("/jobs/1").get_json()["job_id"], "1")

    def test_07b_friendly_recurring_edit(self):
        # The schedule builder pre-fills from the job's current cron (from schedulefire.toml)
        # and can update it without anyone typing cron syntax.
        page = self.client.get("/schedule-fire/schedules", headers=ADMIN)
        self.assertEqual(page.status_code, 200)
        self.assertIn(b'<option value="daily" selected>', page.data)  # "0 2 * * *" -> daily at 02:00
        token = self.csrf("/schedule-fire/schedules")
        res = self.client.post("/schedule-fire/actions/recurring/update", headers=ADMIN, data={
            "id": "nightly-sales-report", "frequency": "daily", "time_daily": "03:30",
            "time_zone": "UTC", "queue": "default", "next": "/schedules", "csrf": token,
        })
        self.assertEqual(res.status_code, 303)
        self.assertEqual(self.storage.get_recurring("nightly-sales-report").cron, "30 3 * * *")

    def test_08_cancel_a_running_job_from_the_dashboard(self):
        res = self.client.post("/exports").get_json()
        job_id, path = res["job_id"], res["path"]
        self.wait_state(job_id, "Processing")
        token = self.csrf(f"/schedule-fire/job/{job_id}")
        deleted = self.client.post("/schedule-fire/actions/jobs/delete", headers=ADMIN,
                                   data={"id": job_id, "next": f"/job/{job_id}", "csrf": token})
        self.assertEqual(deleted.status_code, 303)
        self.assertEqual(self.state(job_id), "Deleted")
        time.sleep(7)       # the job notices within ~5 seconds and stops
        with open(path, encoding="utf-8") as f:
            self.assertNotIn("DONE", f.read())
        self.assertEqual(self.state(job_id), "Deleted")

    # -- 4. security & operations ---------------------------------------------------------

    def test_09_injected_job_is_refused(self):
        from schedulefire.states import EnqueuedState

        victim = os.path.join(TMP, "keep.txt")
        with open(victim, "w") as f:
            f.write("keep")
        job_id = self.storage.create_job({"module": "os", "qualname": "remove", "args": [victim], "kwargs": {}},
                                         "default", {}, expire_in=3600)
        self.storage.set_state(job_id, EnqueuedState(queue="default"))
        self.wait_state(job_id, "Failed")
        self.assertTrue(os.path.exists(victim))
        self.assertIn("JobSecurityError", self.storage.get_job(job_id).state_data["exception_type"])

    def test_10_scale_workers_while_running(self):
        from schedulefire import update_server

        server = self.storage.get_servers()[0]
        self.assertEqual(server.worker_count, 4)
        self.assertTrue(update_server(server.id, worker_count=2, storage=self.storage))
        updated = wait_until(lambda: (lambda s: s if s.worker_count == 2 and not s.requested else None)(
            self.storage.get_server(server.id)), timeout=20)
        self.assertIsNotNone(updated, "the worker did not apply the change")
        placed = self.order("after-scaling@example.com")
        self.wait_state(placed["jobs"]["charge"], "Succeeded")     # still processing with 2 workers
        update_server(server.id, worker_count=4, storage=self.storage)


if __name__ == "__main__":
    unittest.main()

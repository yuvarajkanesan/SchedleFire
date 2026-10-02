"""Everything in one process: create jobs, run a server and the dashboard.

Run from the project root:   python -m examples.quickstart
Then open http://127.0.0.1:8080/schedule-fire/
"""

import logging
from datetime import timedelta

import schedulefire
from schedulefire import BackgroundJob, BackgroundJobServer, Cron, Job, RecurringJob, SQLiteStorage
from schedulefire.dashboard import run_dashboard

from examples import tasks

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")

schedulefire.configure(storage=SQLiteStorage("example.db"))

# Fire-and-forget
job_id = BackgroundJob.enqueue(tasks.send_welcome_email, "ada@example.com", name="Ada")

# Continuation: runs after job_id succeeds
BackgroundJob.continue_job_with(job_id, tasks.charge_card, 42, 19.99)

# Delayed
BackgroundJob.schedule(tasks.call_flaky_api, timedelta(seconds=10), "/v1/orders")

# Instance method and async jobs
BackgroundJob.enqueue(tasks.InvoiceService.generate, 1001)
BackgroundJob.enqueue(tasks.fetch_prices, "ACME")
BackgroundJob.enqueue(tasks.rebuild_search_index)

# Recurring
RecurringJob.add_or_update("nightly-report", tasks.nightly_report, Cron.daily(2, 30))
RecurringJob.add_or_update("ping-api", Job(tasks.call_flaky_api, args=("/health",)), Cron.minutely())

run_dashboard(port=8080, background=True)
BackgroundJobServer(queues=["critical", "default"], worker_count=5).run()  # Ctrl+C to stop

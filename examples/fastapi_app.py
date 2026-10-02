"""Using scheduleFire from a FastAPI app.

    pip install fastapi uvicorn a2wsgi
    uvicorn examples.fastapi_app:app          # from the project root

Then open http://127.0.0.1:8000/docs and http://127.0.0.1:8000/schedule-fire/ (dashboard).

For production, prefer running workers in their own process instead of inside the
web app (set RUN_WORKER=0 here and start `python -m schedulefire server --db app-jobs.db`).
"""

import os
from contextlib import asynccontextmanager
from datetime import timedelta

from a2wsgi import WSGIMiddleware  # the dashboard is WSGI; FastAPI is ASGI
from fastapi import FastAPI

import schedulefire
from schedulefire import BackgroundJob, BackgroundJobServer, Cron, RecurringJob, SQLiteStorage
from schedulefire.dashboard import DashboardApp

from examples import tasks

storage = SQLiteStorage("app-jobs.db")
schedulefire.configure(storage=storage)


@asynccontextmanager
async def lifespan(app: FastAPI):
    RecurringJob.add_or_update("nightly-report", tasks.nightly_report, Cron.daily(2))
    server = None
    if os.environ.get("RUN_WORKER", "1") == "1":
        server = BackgroundJobServer(storage, queues=["critical", "default"], worker_count=5).start()
    yield
    if server is not None:
        server.stop()


app = FastAPI(lifespan=lifespan)

# Dashboard at /schedule-fire. It is open to anyone who can reach it unless you pass auth=BasicAuth(...) or auth=OAuth(...).
app.mount("/schedule-fire", WSGIMiddleware(DashboardApp(storage)))


# Plain `def` endpoints: FastAPI runs them in a thread pool, so the short SQLite
# write done by enqueue() never blocks the event loop.
@app.post("/users/{email}/welcome")
def welcome(email: str, name: str = "friend"):
    job_id = BackgroundJob.enqueue(tasks.send_welcome_email, email, name=name)
    return {"job_id": job_id}


@app.post("/orders/{order_id}/charge")
def charge(order_id: int, amount: float):
    job_id = BackgroundJob.enqueue(tasks.charge_card, order_id, amount)
    BackgroundJob.schedule(tasks.call_flaky_api, timedelta(minutes=5), f"/orders/{order_id}/receipt")
    return {"job_id": job_id}


@app.get("/jobs-api/{job_id}")
def job_status(job_id: str):
    record = storage.get_job(job_id)
    if record is None:
        return {"job_id": job_id, "state": None}
    return {"job_id": job_id, "state": record.state_name, "result": record.state_data.get("result")}

"""Using scheduleFire from a Flask app.

    pip install flask
    flask --app examples.flask_app run                  # from the project root
    python -m schedulefire server --db app-jobs.db            # workers, in a second terminal

Then open http://127.0.0.1:5000/schedule-fire/ for the dashboard.

Workers run in their own process here. Starting a BackgroundJobServer inside the web
app also works, but the dev server's auto-reloader and multi-process servers
(gunicorn -w 4) would start one job server per process.
"""

from datetime import timedelta

from flask import Flask, jsonify, request

import schedulefire
from schedulefire import BackgroundJob, Cron, RecurringJob, SQLiteStorage
from schedulefire.dashboard import mount_dashboard

from examples import tasks

storage = SQLiteStorage("app-jobs.db")
schedulefire.configure(storage=storage)
RecurringJob.add_or_update("nightly-report", tasks.nightly_report, Cron.daily(2))

app = Flask(__name__)
# Dashboard at /schedule-fire. It is open unless you pass auth=BasicAuth(...) / auth=OAuth(...); routes={...} renames pages.
app.wsgi_app = mount_dashboard(app.wsgi_app, storage=storage)   # -> /schedule-fire


@app.post("/users/<email>/welcome")
def welcome(email):
    job_id = BackgroundJob.enqueue(tasks.send_welcome_email, email, name=request.args.get("name", "friend"))
    return jsonify(job_id=job_id)


@app.post("/orders/<int:order_id>/charge")
def charge(order_id):
    job_id = BackgroundJob.enqueue(tasks.charge_card, order_id, float(request.args["amount"]))
    BackgroundJob.schedule(tasks.call_flaky_api, timedelta(minutes=5), f"/orders/{order_id}/receipt")
    return jsonify(job_id=job_id)


@app.get("/jobs-api/<job_id>")
def job_status(job_id):
    record = storage.get_job(job_id)
    return jsonify(job_id=job_id, state=record and record.state_name,
                   result=record and record.state_data.get("result"))

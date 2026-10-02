"""The shop's web app (Flask). Run with:  flask --app shop.web run

Everything scheduleFire needs comes from schedulefire.toml; the dashboard is mounted
at /schedule-fire with the login, routes and custom page configured below.
"""

import os
from datetime import timedelta
from html import escape

from flask import Flask, jsonify, redirect, request, url_for

from schedulefire import BackgroundJob, get_storage, load_config
from schedulefire.dashboard import DashboardApp, mount_dashboard

from . import jobs, store

settings = load_config()          # finds ./schedulefire.toml (or $SCHEDULEFIRE_CONFIG)
settings.apply()                  # storage, retry policy and job allowlist for this process

REVIEW_DELAY = float(os.environ.get("SHOP_REVIEW_DELAY", "30"))   # seconds

app = Flask(__name__)

# ---- the scheduleFire dashboard, inside this app ------------------------------------
dashboard = DashboardApp(**settings.dashboard.app_kwargs())      # basic login + custom routes


@dashboard.page("/shop", "Shop orders", icon="overview")
def shop_page(ctx):
    summary = store.sales_summary()
    rows = "".join(
        f"<tr><td>#{o['id']}</td><td>{ctx.escape(o['email'])}</td><td>{o['amount']:.2f}</td>"
        f"<td>{ctx.escape(o['status'])}</td></tr>" for o in store.all_orders()[:20])
    return (f'<section class="card"><div class="card-body">{summary["orders"]} orders · '
            f'revenue {summary["revenue"]:.2f}</div><div class="table-wrap"><table class="t"><thead><tr>'
            f'<th>Order</th><th>Email</th><th>Amount</th><th>Status</th></tr></thead><tbody>{rows}</tbody>'
            f'</table></div></section>')


app.wsgi_app = mount_dashboard(app.wsgi_app, dashboard=dashboard)    # -> /schedule-fire


# ---- the shop -----------------------------------------------------------------------
def place_order(email: str, amount: float) -> dict:
    order_id = store.create_order(email, amount)
    confirmation = BackgroundJob.enqueue(jobs.send_confirmation_email, order_id)                  # fire-and-forget
    charge = BackgroundJob.enqueue(jobs.charge_payment, order_id)                                 # critical queue
    invoice = BackgroundJob.continue_job_with(charge, jobs.generate_invoice, order_id)            # continuation
    review = BackgroundJob.schedule(jobs.send_review_request, timedelta(seconds=REVIEW_DELAY), order_id)  # delayed
    return {"order_id": order_id, "jobs": {"confirmation": confirmation, "charge": charge,
                                           "invoice": invoice, "review": review}}


@app.get("/")
def home():
    rows = "".join(
        f"<tr><td>#{o['id']}</td><td>{escape(o['email'])}</td><td>{o['amount']:.2f}</td><td>{escape(o['status'])}</td>"
        f"<td>{'yes' if o['confirmation_sent'] else '-'}</td><td>{escape(o['invoice'] or '-')}</td></tr>"
        for o in store.all_orders())
    return f"""<!doctype html><meta charset="utf-8"><title>Demo shop</title>
<style>body{{font:15px system-ui;margin:32px;max-width:900px}}td,th{{padding:6px 10px;border-bottom:1px solid #ddd;text-align:left}}</style>
<h1>Demo shop</h1>
<p>Place an order: its email, payment, invoice and review request run as background jobs.
Watch them in the <a href="/schedule-fire/">scheduleFire dashboard</a>.</p>
<form method="post" action="/orders">
  <input name="email" value="ada@example.com" size="30"> <input name="amount" value="49.90" size="8">
  <button>Place order</button>
</form>
<p><small>Try an email starting with <b>flaky</b> (payment fails twice, then succeeds) or <b>declined</b> (fails, no retry).</small></p>
<table><tr><th>Order</th><th>Email</th><th>Amount</th><th>Status</th><th>Confirmation</th><th>Invoice</th></tr>{rows}</table>"""


@app.post("/orders")
def create_order():
    data = request.get_json(silent=True) or request.form
    result = place_order(data.get("email", "ada@example.com"), float(data.get("amount", 49.9)))
    if request.is_json:
        return jsonify(result), 202
    return redirect(url_for("home"))


@app.get("/orders/<int:order_id>")
def order_status(order_id):
    return jsonify(store.get(order_id))


@app.post("/exports")
def export_orders():
    path = os.path.join(os.path.dirname(store._db_path()) or ".", "exports", "orders.csv")
    return jsonify(job_id=BackgroundJob.enqueue(jobs.export_all_orders, path), path=path), 202


@app.get("/jobs/<job_id>")
def job_state(job_id):
    """A route of the shop's own that happens to be called /jobs - no clash with the dashboard."""
    record = get_storage().get_job(job_id)
    return jsonify(job_id=job_id, state=record.state_name if record else None)

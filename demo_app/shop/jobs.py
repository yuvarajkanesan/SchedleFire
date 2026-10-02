"""Background jobs for the shop. Workers import this module by name (see schedulefire.toml).

Every job is idempotent: running it twice (e.g. after a worker crash) does no harm.
"""

import os
import time

from schedulefire import AutomaticRetry, DisableConcurrentExecution, get_job_context, queue

from . import store


class PaymentDeclined(Exception):
    """The card was declined. Retrying won't help, so it is excluded from retries."""


def send_confirmation_email(order_id: int) -> str:
    """Fire-and-forget: runs on the default queue as soon as a worker is free."""
    order = store.get(order_id)
    if order["confirmation_sent"]:
        return "already sent"
    # (a real app would call its mail service here)
    store.update(order_id, confirmation_sent=1)
    return f"confirmation sent to {order['email']}"


@queue("critical")
@AutomaticRetry(attempts=3, delays=[1, 1, 1], except_on=(PaymentDeclined,))
def charge_payment(order_id: int) -> dict:
    """Critical queue, retried on gateway errors, never retried when declined."""
    order = store.get(order_id)
    if order["status"] in ("paid", "invoiced"):
        return {"charged": order["amount"], "note": "already charged"}
    attempt = store.add_payment_attempt(order_id)
    if order["email"].startswith("declined"):
        raise PaymentDeclined(f"card declined for order {order_id}")
    if order["email"].startswith("flaky") and attempt < 3:
        raise ConnectionError(f"payment gateway timed out (attempt {attempt})")
    store.update(order_id, status="paid")
    return {"charged": order["amount"], "attempts": attempt}


def generate_invoice(order_id: int) -> str:
    """Continuation: only runs after charge_payment succeeded."""
    invoice = f"INV-{order_id:05d}.pdf"
    store.update(order_id, invoice=invoice, status="invoiced")
    return invoice


@queue("low")
def send_review_request(order_id: int) -> str:
    """Delayed job: scheduled a while after the order."""
    store.update(order_id, review_requested=1)
    return f"review request sent for order {order_id}"


def nightly_sales_report() -> dict:
    """Recurring job (see [[recurring]] in schedulefire.toml)."""
    return store.sales_summary()


def cleanup_abandoned_carts() -> int:
    """Recurring job every 10 minutes."""
    return 0


@queue("low")
@DisableConcurrentExecution(timeout=60)
def export_all_orders(path: str, steps: int = 200) -> str:
    """Long-running and cancellable: stops cleanly when deleted from the dashboard or on shutdown."""
    ctx = get_job_context()
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("id,email,amount,status\n")
        for step in range(steps):
            ctx.throw_if_cancellation_requested()
            time.sleep(0.05)
            f.write(f"step {step}\n")
            f.flush()
        for order in store.all_orders():
            f.write(f"{order['id']},{order['email']},{order['amount']},{order['status']}\n")
        f.write("DONE\n")
    return path

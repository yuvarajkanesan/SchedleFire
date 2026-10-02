"""Job functions. Jobs must be importable functions/methods - the worker imports them by name."""

import logging
import random
import time

from schedulefire import AutomaticRetry, DisableConcurrentExecution, get_job_context, queue

log = logging.getLogger("examples")


def send_welcome_email(email: str, name: str) -> str:
    log.info("Sending welcome email to %s <%s>", name, email)
    time.sleep(0.5)
    return f"sent to {email}"


@queue("critical")
def charge_card(order_id: int, amount: float) -> None:
    log.info("Charging order %s: %.2f", order_id, amount)


@AutomaticRetry(attempts=3, delays=[2, 5, 10])
def call_flaky_api(endpoint: str) -> int:
    if random.random() < 0.6:
        raise ConnectionError(f"{endpoint} timed out")
    return 200


@DisableConcurrentExecution(timeout=60)
def rebuild_search_index() -> None:
    ctx = get_job_context()
    for step in range(10):
        ctx.throw_if_cancellation_requested()  # stop cleanly on shutdown / delete
        time.sleep(0.3)
    log.info("Search index rebuilt (job #%s)", ctx.job_id)


def nightly_report() -> None:
    log.info("Building nightly report")


class InvoiceService:
    """Instance-method jobs: the worker creates the instance via the JobActivator."""

    def generate(self, invoice_id: int) -> str:
        return f"invoice-{invoice_id}.pdf"


async def fetch_prices(symbol: str) -> float:
    import asyncio

    await asyncio.sleep(0.2)
    return 101.5

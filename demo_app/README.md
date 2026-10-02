# Demo shop: scheduleFire in a real app

A small Flask shop that uses scheduleFire **as an installed package** (the exported wheel), exactly as
your own projects would. Placing an order starts a chain of background jobs that uses every major feature.

| Feature | Where in the demo |
|---|---|
| Fire-and-forget | `send_confirmation_email` is enqueued for every order |
| Queues and priority | `charge_payment` → `critical`, `send_review_request` / `export_all_orders` → `low` |
| Retries | `charge_payment` retries gateway errors (emails starting with `flaky` fail twice, then succeed) |
| No retry for some errors | `PaymentDeclined` is excluded (emails starting with `declined`) |
| Continuation | `generate_invoice` runs only after the payment succeeds |
| Delayed job | `send_review_request` runs `SHOP_REVIEW_DELAY` seconds after the order (default 30) |
| Recurring jobs | `nightly-sales-report` and `cleanup-abandoned-carts`, defined in `schedulefire.toml` |
| Cancellation | `export_all_orders` stops cleanly when deleted from the dashboard |
| One at a time | `export_all_orders` uses `@DisableConcurrentExecution` |
| Job allowlist | `[security] allowed_job_modules = ["shop.jobs"]` |
| Dashboard in the app | mounted at `/schedule-fire` with a login, a renamed page (`/schedules`) and a custom page (`/shop`) |
| No route clashes | the shop has its own `/jobs/<id>` route next to the dashboard |
| Friendly recurring-job editing | change `nightly-sales-report` / `cleanup-abandoned-carts`'s schedule from `/schedules` with a frequency picker - no cron syntax needed |

## Run it

From this folder (`demo_app`), in two terminals:

```powershell
# once: create the environment and install Flask + the scheduleFire wheel
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt

# terminal 1: the web app
$env:SHOP_ADMIN_PASSWORD = "choose-a-password"
.venv\Scripts\flask --app shop.web run

# terminal 2: the workers
.venv\Scripts\schedulefire server
```

Open http://127.0.0.1:5000/ and place a few orders; try emails starting with `flaky` and `declined`.
Watch them at http://127.0.0.1:5000/schedule-fire/ (user `admin`, the password you set).

`requirements.txt` installs `../dist/schedulefire-0.1.0-py3-none-any.whl`. After changing the library, rebuild it
from the library folder with `pip wheel . --no-deps -w dist`, then reinstall it here with
`.venv\Scripts\python -m pip install --force-reinstall ..\dist\schedulefire-0.1.0-py3-none-any.whl`.

## Test it

```powershell
.venv\Scripts\python -m unittest -v
```

The tests use throwaway databases and start a real worker (the installed `schedulefire` command). They check:

1. scheduleFire is imported from this environment's `site-packages`, not from the source folder
2. `schedulefire config check` validates the config and connects
3. an order runs every job type: confirmation, payment on `critical`, invoice as a continuation, review as a delayed job on `low`
4. a flaky payment is retried twice, then succeeds, and the invoice follows
5. a declined payment fails immediately without retries, and no invoice is created
6. recurring jobs from the config exist, and "Trigger now" from the dashboard runs one
7. the dashboard requires the login, uses the custom routes, shows the custom page, and leaves the shop's routes alone
7b. the recurring-job schedule editor pre-fills from the existing cron and updates it without typing cron syntax
8. deleting a running export from the dashboard stops it before it finishes
9. a malicious job written straight into the database (`os.remove`) is refused and the file survives
10. the worker count of the running server can be changed live, and jobs keep flowing

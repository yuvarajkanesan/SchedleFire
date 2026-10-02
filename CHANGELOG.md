# Changelog

All notable changes to scheduleFire are documented here.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning follows [Semantic Versioning](https://semver.org/) once 1.0.0 ships.
Before 1.0.0, minor versions may include breaking changes.

## [Unreleased]

## [0.1.0] - 2026-09-25

Initial release.

### Added
- Fire-and-forget, delayed, recurring and continuation jobs, modelled on .NET's Hangfire
  (`BackgroundJob`, `RecurringJob`, `BackgroundJobClient`, `RecurringJobManager`).
- Storage backends: SQLite and in-memory built in; PostgreSQL, MySQL/MariaDB, SQL Server and
  Redis behind optional extras. Automatic SQLite fallback when no database is configured.
- A job filter pipeline (client/server/elect/apply filters), `AutomaticRetry` with configurable
  backoff, `DisableConcurrentExecution` distributed locks, and job parameters.
- `BackgroundJobServer`: multi-threaded workers, live worker/queue scaling, heartbeats and
  dead-server cleanup, graceful shutdown, and a `BackgroundProcess` extension point for custom
  recurring tasks.
- A self-contained web dashboard (no CDN dependencies): job lists and details, a friendly
  recurring-job schedule editor (frequency picker + plain fields, no cron syntax required for
  the common cases), rescheduling a scheduled job relatively or to an exact time, server and
  queue views, bulk actions, and dark mode.
- Dashboard authentication: none, local-only, HTTP Basic (with a proper sign-in form and
  configurable idle timeout), and OAuth/OIDC (Google, Microsoft, GitHub, or any OpenID Connect
  provider).
- A job security policy that refuses to run stdlib functions, lambdas or nested functions by
  default, with an explicit module allowlist for production.
- A `schedulefire.toml` / `schedulefire.json` configuration file covering storage, retries,
  server, dashboard, security and recurring jobs, plus a `schedulefire` CLI.
- Support for Python 3.7 through 3.14.

### Known limitations
- The SQL Server backend has not yet been exercised against a live server in CI or by a real
  deployment; treat it as beta until validated in your environment.
- Batches / batch continuations (Hangfire Pro's grouped-jobs feature) are not implemented.
- Recurring jobs always use "catch up once" semantics for missed schedules (Hangfire's
  `MisfireHandlingMode.Relaxed`); `Strict` and `Ignorable` modes are not configurable.

[Unreleased]: https://example.invalid/schedulefire/compare/v0.1.0...HEAD
[0.1.0]: https://example.invalid/schedulefire/releases/tag/v0.1.0

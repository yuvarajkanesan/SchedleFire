# Security policy

## Supported versions

scheduleFire is pre-1.0; the latest published release is the only one that receives security
fixes. Once 1.0.0 ships, this section will list which major versions remain supported.

## Reporting a vulnerability

Please report suspected security vulnerabilities privately - **do not open a public GitHub
issue**. Email **\<your-security-contact-email\>** with:

- a description of the issue and its potential impact,
- steps to reproduce (a minimal script or config is ideal),
- the scheduleFire version and storage backend affected.

You should receive an acknowledgement within 5 business days. We'll work with you to understand
and confirm the issue, prepare a fix, and agree on a disclosure timeline before any public
announcement.

## Scope and known considerations

- **Job security policy.** By default, scheduleFire refuses to run standard-library functions,
  lambdas or nested functions as jobs, since job storage is otherwise just "module + function
  name + JSON arguments" that anyone with write access to it could abuse to call arbitrary code.
  For production, set `allowed_job_modules` (or `[security] allowed_job_modules` in the config
  file) to an explicit allowlist. Treat write access to your job storage as equivalent to code
  execution access on your workers.
- **Dashboard authentication.** The dashboard has no authentication by default and will log a
  warning if it's reachable from outside `127.0.0.1` without one configured. Always set `auth=`
  (Basic or OAuth/OIDC) before exposing it beyond localhost, and put it behind HTTPS - credentials
  and job data are sent in the clear over plain HTTP otherwise.
- **Secrets in configuration.** Connection strings and dashboard secrets support `${ENV_VAR}`
  substitution in the config file so secrets don't need to be committed; error messages and logs
  redact known secret fields.

If you're a corporate user with an internal vulnerability disclosure process instead, feel free to
use that in place of the email above - the important thing is a private report before any public
disclosure.

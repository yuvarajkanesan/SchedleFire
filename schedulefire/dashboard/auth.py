"""Dashboard authentication.

* :class:`NoAuth` - open to anyone who can reach the dashboard (the default).
* :class:`LocalOnlyAuth` - only browsers on the same machine (no reverse proxy).
* :class:`BasicAuth` - HTTP Basic with one or more users; passwords may be stored as
  PBKDF2 hashes (``schedulefire hash-password``).
* :class:`OAuth` - "Sign in with Google / Microsoft / GitHub / any OpenID Connect
  provider": authorization-code flow with PKCE, signed session cookies, and an
  allowlist of who may enter.

Everything uses the standard library only.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from http.cookies import CookieError, SimpleCookie
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple, Union
from urllib.parse import parse_qs, urlencode, urlsplit

logger = logging.getLogger("schedulefire.dashboard")

Response = Tuple[str, List[Tuple[str, str]], bytes]

SESSION_COOKIE = "schedulefire_session"
FLOW_COOKIE = "schedulefire_oauth"

#: How long a browser session may sit idle before it's signed out (BasicAuth's login
#: form and OAuth). ``None`` (or 0) disables idle sign-out.
DEFAULT_IDLE_TIMEOUT = 300.0

_PROXY_HEADERS = ("HTTP_X_FORWARDED_FOR", "HTTP_FORWARDED", "HTTP_X_REAL_IP", "HTTP_X_FORWARDED_HOST",
                  "HTTP_X_CLIENT_IP", "HTTP_CF_CONNECTING_IP", "HTTP_TRUE_CLIENT_IP")


# ------------------------------------------------------------------ request helpers


class Request:
    """A thin view over the WSGI environ."""

    def __init__(self, environ: dict):
        self.environ = environ
        self.method = environ.get("REQUEST_METHOD", "GET").upper()
        self.path = environ.get("PATH_INFO", "") or "/"
        self.query = parse_qs(environ.get("QUERY_STRING", ""))
        self.base = environ.get("SCRIPT_NAME", "").rstrip("/")
        self.remote_addr = environ.get("REMOTE_ADDR", "")
        self.cookies: Dict[str, str] = {}
        try:
            jar = SimpleCookie()
            jar.load(environ.get("HTTP_COOKIE", ""))
            self.cookies = {k: m.value for k, m in jar.items()}
        except CookieError:
            pass
        #: Set-Cookie (etc.) headers an auth provider wants added to whatever response this
        #: request ends up producing - e.g. a refreshed idle-timeout session cookie. The app
        #: splices these into every response, not just ones the auth layer builds itself.
        self.extra_headers: List[Tuple[str, str]] = []

    def header(self, name: str) -> str:
        return self.environ.get("HTTP_" + name.upper().replace("-", "_"), "")

    @property
    def is_local(self) -> bool:
        """A browser on this machine talking to us directly (not through a proxy)."""
        if any(self.environ.get(h) for h in _PROXY_HEADERS):
            return False
        addr = self.remote_addr
        return addr in ("::1", "localhost", "::ffff:127.0.0.1") or addr.startswith("127.")

    @property
    def is_https(self) -> bool:
        forwarded = self.header("X-Forwarded-Proto").split(",")[0].strip().lower()
        return forwarded == "https" or self.environ.get("wsgi.url_scheme") == "https"

    @property
    def host(self) -> str:
        forwarded = self.header("X-Forwarded-Host").split(",")[0].strip()
        if forwarded:
            return forwarded
        if self.header("Host"):
            return self.header("Host")
        port = self.environ.get("SERVER_PORT", "")
        default = "443" if self.environ.get("wsgi.url_scheme") == "https" else "80"
        return self.environ.get("SERVER_NAME", "localhost") + ("" if port in ("", default) else f":{port}")

    def absolute_url(self, path: str) -> str:
        return f"{'https' if self.is_https else 'http'}://{self.host}{self.base}{path}"

    @property
    def wants_html(self) -> bool:
        return self.method in ("GET", "HEAD") and not self.header("X-Requested-With")


@dataclass
class User:
    id: str
    name: str
    email: Optional[str] = None
    provider: str = ""

    @property
    def display(self) -> str:
        return self.name or self.email or self.id


def cookie_header(name: str, value: str, request: Request, *, max_age: Optional[int] = None,
                  same_site: str = "Lax") -> Tuple[str, str]:
    parts = [f"{name}={value}", f"Path={request.base or '/'}", "HttpOnly", f"SameSite={same_site}"]
    if max_age is not None:
        parts.append(f"Max-Age={int(max_age)}")
    if request.is_https:
        parts.append("Secure")
    return "Set-Cookie", "; ".join(parts)


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _unb64(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def sign(data: Dict[str, Any], key: bytes) -> str:
    payload = _b64(json.dumps(data, separators=(",", ":")).encode("utf-8"))
    return payload + "." + _b64(hmac.new(key, payload.encode("ascii"), hashlib.sha256).digest())


def unsign(value: Optional[str], key: bytes) -> Optional[Dict[str, Any]]:
    """The signed data, or None if missing, tampered with or expired."""
    if not value or "." not in value:
        return None
    payload, _, signature = value.rpartition(".")
    expected = _b64(hmac.new(key, payload.encode("ascii"), hashlib.sha256).digest())
    if not hmac.compare_digest(signature, expected):
        return None
    try:
        data = json.loads(_unb64(payload))
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(data, dict) or data.get("exp", 0) < time.time():
        return None
    return data


def safe_next(path: Optional[str]) -> str:
    """Only same-site paths ("//x" and "/\\x" are other hosts to a browser)."""
    if not path or not path.startswith("/") or path[1:2] in ("/", "\\") or any(ord(c) < 32 for c in path):
        return "/"
    return path


def text_response(status: str, text: str, headers: Sequence[Tuple[str, str]] = ()) -> Response:
    return status, [("Content-Type", "text/plain; charset=utf-8"), *headers], text.encode("utf-8")


def format_duration(seconds: float) -> str:
    """``305`` -> "5 minutes"; used in "you were signed out after ..." messages."""
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds} second{'s' if seconds != 1 else ''}"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} minute{'s' if minutes != 1 else ''}"
    hours = minutes // 60
    return f"{hours} hour{'s' if hours != 1 else ''}"


def _resolve_secret_key(secret_key: Optional[str], who: str) -> bytes:
    """32+ char string -> a signing key; ``None`` -> a per-process random one (with a warning:
    sessions won't survive a restart or work across several dashboard processes)."""
    if secret_key is None:
        logger.warning("%s: no secret_key set - using a random one. Sessions end when this process "
                       "restarts and don't work across several dashboard processes.", who)
        return secrets.token_bytes(32)
    if len(secret_key) < 32:
        raise ValueError(f"{who}: secret_key must be at least 32 characters (e.g. `python -c \"import "
                         f"secrets; print(secrets.token_urlsafe(48))\"`)")
    return hashlib.sha256(secret_key.encode("utf-8")).digest()


# ------------------------------------------------------------------ passwords


def hash_password(password: str, *, iterations: int = 600_000) -> str:
    """PBKDF2-SHA256 hash for dashboard passwords: ``pbkdf2_sha256$iterations$salt$hash``."""
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${_b64(salt)}${_b64(digest)}"


def verify_password(password: str, stored: str) -> bool:
    if stored.startswith("pbkdf2_sha256$"):
        try:
            _, iterations, salt, digest = stored.split("$")
            actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), _unb64(salt), int(iterations))
            return hmac.compare_digest(actual, _unb64(digest))
        except (ValueError, TypeError):
            return False
    return hmac.compare_digest(password.encode("utf-8"), stored.encode("utf-8"))


_DUMMY_HASH = hash_password("schedulefire-dummy", iterations=1000)


# ------------------------------------------------------------------ auth providers


class DashboardAuth:
    """Base class. ``authenticate`` returns a User, None (anonymous access allowed), or a
    Response to send instead of the page (401, 403, a redirect to the login page, ...)."""

    name = "custom"
    #: True when the dashboard should show a "Sign out" button.
    supports_logout = False
    #: Seconds of inactivity before a browser session is signed out. None = no idle limit.
    #: Only meaningful for auth modes that keep a session (BasicAuth's login form, OAuth).
    idle_timeout: Optional[float] = None

    def authenticate(self, request: Request, login_url: str) -> Union[User, None, Response]:
        raise NotImplementedError

    def logout(self, request: Request) -> Tuple[str, str]:
        """Clear the session cookie. The default clears the shared cookie both BasicAuth's
        login form and OAuth use; override if your auth keeps its session elsewhere."""
        return cookie_header(SESSION_COOKIE, "", request, max_age=0)

    def describe(self) -> str:
        return self.name


class NoAuth(DashboardAuth):
    """No authentication: anyone who can reach the dashboard may use it."""

    name = "none"

    def __init__(self):
        self._warned = False

    def authenticate(self, request, login_url):
        if not self._warned and not request.is_local:
            self._warned = True
            logger.warning("The dashboard is being used from %s WITHOUT authentication. Enable basic or oauth "
                           "authentication if it is reachable by others.", request.remote_addr or "a remote host")
        return None


class LocalOnlyAuth(DashboardAuth):
    """Only browsers on the same machine, connecting directly (not through a proxy)."""

    name = "local"

    def authenticate(self, request, login_url):
        if request.is_local:
            return None
        return text_response("403 Forbidden", "Forbidden. This dashboard only accepts requests from the local "
                                              "machine; enable basic or oauth authentication for remote access.")


class _LoginThrottle:
    """Too many failed logins from one address -> 429 for a while."""

    def __init__(self, limit: int = 10, window: float = 300.0):
        self.limit, self.window = limit, window
        self._failures: Dict[str, List[float]] = {}
        self._lock = threading.Lock()

    def _recent(self, key: str, now: float) -> List[float]:
        entries = [t for t in self._failures.get(key, []) if now - t < self.window]
        if entries:
            self._failures[key] = entries
        else:
            self._failures.pop(key, None)
        return entries

    def retry_after(self, key: str) -> int:
        now = time.time()
        with self._lock:
            entries = self._recent(key, now)
            return int(self.window - (now - entries[0])) + 1 if len(entries) >= self.limit else 0

    def failed(self, key: str) -> None:
        with self._lock:
            now = time.time()
            self._recent(key, now)
            self._failures.setdefault(key, []).append(now)
            if len(self._failures) > 10000:
                self._failures.pop(next(iter(self._failures)))

    def succeeded(self, key: str) -> None:
        with self._lock:
            self._failures.pop(key, None)


class BasicAuth(DashboardAuth):
    """Username/password authentication.

    A browser gets a proper HTML sign-in page (not the browser's native Basic-auth popup):
    submitting it starts a signed, ``HttpOnly`` session cookie, the same mechanism OAuth uses,
    including the idle-timeout below and a "Sign out" button. A request that already carries
    an ``Authorization: Basic`` header (``curl -u``, scripts, monitoring) is still accepted
    directly and statelessly - no cookie, no idle timeout, since there is no "session" to expire
    when credentials are resent on every request.

    :param users: ``{"name": "password or pbkdf2_sha256$... hash"}``. Store hashes made with
        ``schedulefire hash-password`` so the config never holds plain passwords.
    :param idle_timeout: seconds of inactivity before a browser session is signed out
        (default 5 minutes). ``None`` or ``0`` disables it. A background "keep the dashboard
        live-refreshing" poll does not count as activity - only real navigation, actions, and
        the sign-in-warning's "Stay signed in" button do.
    :param session_lifetime: absolute cap on a session's age regardless of activity (default
        8 hours) - a second line of defense that forces periodic re-authentication.
    :param secret_key: signs the session cookie. Give every dashboard process the same key
        (e.g. from an environment variable) so sessions survive restarts and load balancing.
    """

    name = "basic"
    supports_logout = True

    def __init__(self, users: Optional[Mapping[str, str]] = None, *, username: Optional[str] = None,
                 password: Optional[str] = None, realm: str = "scheduleFire",
                 idle_timeout: Optional[float] = DEFAULT_IDLE_TIMEOUT, session_lifetime: float = 8 * 3600,
                 secret_key: Optional[str] = None):
        combined = dict(users or {})
        if username is not None:
            if not password:
                raise ValueError("BasicAuth: a password is required")
            combined[username] = password
        if not combined:
            raise ValueError("BasicAuth needs at least one user")
        for user, secret in combined.items():
            if not user or ":" in user or not secret:
                raise ValueError(f"BasicAuth: invalid user entry {user!r}")
        self.users = combined
        self.realm = realm
        self.idle_timeout = idle_timeout or None
        self.session_lifetime = session_lifetime
        self.secret_key = _resolve_secret_key(secret_key, "BasicAuth")
        self.throttle = _LoginThrottle()

    def describe(self) -> str:
        text = f"basic ({len(self.users)} user{'s' if len(self.users) != 1 else ''})"
        return text + f", idle timeout {format_duration(self.idle_timeout)}" if self.idle_timeout else text

    def _check_credentials(self, user: str, password: str) -> bool:
        stored = self.users.get(user)
        # Always verify something so timing doesn't reveal whether the user exists.
        return verify_password(password, stored if stored is not None else _DUMMY_HASH) and stored is not None

    def verify_login(self, request: Request, username: str, password: str) -> Union[User, None, Response]:
        """Core of the sign-in form's POST handler: a User, None (wrong credentials), or a
        429 Response (too many recent failures)."""
        client = request.remote_addr
        wait = self.throttle.retry_after(client)
        if wait:
            return text_response("429 Too Many Requests", "Too many failed logins. Try again later.",
                                 [("Retry-After", str(wait))])
        if self._check_credentials(username, password):
            self.throttle.succeeded(client)
            return User(id=username, name=username, provider="basic")
        self.throttle.failed(client)
        logger.warning("Dashboard login failed for user %r from %s", username[:64], client)
        return None

    def start_session(self, request: Request, user_id: str) -> Tuple[str, str]:
        """The Set-Cookie header for a freshly signed-in browser session."""
        now = time.time()
        payload = {"sub": user_id, "aud": f"basic:{self.realm}", "exp": now + self.session_lifetime,
                  "last_seen": now}
        return cookie_header(SESSION_COOKIE, sign(payload, self.secret_key), request,
                             max_age=int(self.session_lifetime))

    def _session_data(self, request: Request) -> Optional[Dict[str, Any]]:
        data = unsign(request.cookies.get(SESSION_COOKIE), self.secret_key)
        if not data or data.get("aud") != f"basic:{self.realm}" or data.get("sub") not in self.users:
            return None
        return data

    def _touch(self, request: Request, data: Dict[str, Any]) -> None:
        refreshed = dict(data, last_seen=time.time())
        remaining = max(1, int(refreshed["exp"] - time.time()))
        request.extra_headers.append(cookie_header(SESSION_COOKIE, sign(refreshed, self.secret_key), request,
                                                    max_age=remaining))

    def authenticate(self, request, login_url):
        header = request.header("Authorization")
        if header.startswith("Basic "):
            # Stateless: credentials are resent on every request, so no session/idle-timeout applies.
            client = request.remote_addr
            wait = self.throttle.retry_after(client)
            if wait:
                return text_response("429 Too Many Requests", "Too many failed logins. Try again later.",
                                     [("Retry-After", str(wait))])
            try:
                user, _, password = base64.b64decode(header[6:], validate=True).decode("utf-8").partition(":")
            except (ValueError, UnicodeDecodeError):
                user, password = "", ""
            if self._check_credentials(user, password):
                self.throttle.succeeded(client)
                return User(id=user, name=user, provider="basic")
            self.throttle.failed(client)
            logger.warning("Dashboard login failed for user %r from %s", user[:64], client)
            return text_response("401 Unauthorized", "Unauthorized",
                                 [("WWW-Authenticate", f'Basic realm="{self.realm}", charset="UTF-8"')])

        # Browser flow: a signed session cookie from the sign-in form.
        data = self._session_data(request)
        idle = False
        if data is not None:
            idle = bool(self.idle_timeout) and time.time() - data.get("last_seen", 0) > self.idle_timeout
            if not idle:
                if request.method == "POST" or request.wants_html:
                    self._touch(request, data)
                return User(id=data["sub"], name=data["sub"], provider="basic")
        if request.wants_html:
            target = request.path + ("?" + request.environ["QUERY_STRING"] if request.environ.get("QUERY_STRING") else "")
            params = {"next": target}
            if idle:
                params["reason"] = "idle"
            return "302 Found", [("Location", f"{login_url}?{urlencode(params)}")], b""
        return text_response("401 Unauthorized", "Sign-in required.")


class OAuthError(Exception):
    pass


PROVIDERS: Dict[str, Dict[str, Any]] = {
    "google": {"issuer": "https://accounts.google.com", "scopes": ["openid", "email", "profile"],
               "authorize_params": {"prompt": "select_account"}},
    "microsoft": {"issuer": "https://login.microsoftonline.com/{tenant}/v2.0",
                  "scopes": ["openid", "email", "profile"], "tenant": "common"},
    "github": {"authorize_url": "https://github.com/login/oauth/authorize",
               "token_url": "https://github.com/login/oauth/access_token",
               "userinfo_url": "https://api.github.com/user",
               "emails_url": "https://api.github.com/user/emails",
               "scopes": ["read:user", "user:email"]},
    # Any OpenID Connect provider (Okta, Auth0, Keycloak, Authentik, AWS Cognito, ...): give `issuer`.
    "oidc": {"scopes": ["openid", "email", "profile"]},
}
_MULTI_TENANT = {"common", "organizations", "consumers"}


def _http_json(url: str, *, data: Optional[Dict[str, str]] = None, token: Optional[str] = None,
               timeout: float = 10.0) -> Any:
    parts = urlsplit(url)
    if parts.scheme != "https" and parts.hostname not in ("localhost", "127.0.0.1", "::1"):
        raise OAuthError(f"Refusing to call {url}: OAuth endpoints must use https")
    headers = {"Accept": "application/json", "User-Agent": "scheduleFire"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    body = urlencode(data).encode("ascii") if data is not None else None
    if body is not None:
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    request = urllib.request.Request(url, data=body, headers=headers, method="POST" if body is not None else "GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as resp:  # noqa: S310 - https enforced above
            return json.loads(resp.read(1_000_000).decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            detail = e.read(2000).decode("utf-8", "replace")
        finally:
            e.close()
        raise OAuthError(f"{parts.hostname} answered HTTP {e.code}: {detail[:300]}") from None
    except (urllib.error.URLError, OSError, ValueError) as e:
        raise OAuthError(f"Could not reach {parts.hostname}: {e}") from None


class OAuth(DashboardAuth):
    """Sign in with an OAuth 2.0 / OpenID Connect provider.

    ::

        OAuth("google", client_id=..., client_secret=..., allowed_domains=["example.com"],
              secret_key=os.environ["DASHBOARD_SECRET_KEY"])

    Register ``https://<your host>/<dashboard path>/auth/callback`` as the redirect URI at
    the provider (or pass ``redirect_uri``).

    Only users matching ``allowed_emails`` / ``allowed_domains`` / ``allowed_users`` get in;
    emails are only trusted when the provider marks them verified. ``allow_any_user=True``
    admits every account the provider accepts - with Google or GitHub that is anyone.

    ``secret_key`` signs the session cookies. Give every dashboard process the same key
    (e.g. from an environment variable) so sessions survive restarts and load balancing.

    :param idle_timeout: seconds of inactivity before a session is signed out (default 5
        minutes, independent of ``session_lifetime``'s absolute cap). ``None``/``0`` disables it.
    """

    name = "oauth"
    supports_logout = True

    def __init__(
        self,
        provider: str = "oidc",
        *,
        client_id: str,
        client_secret: Optional[str] = None,
        issuer: Optional[str] = None,
        tenant: Optional[str] = None,
        authorize_url: Optional[str] = None,
        token_url: Optional[str] = None,
        userinfo_url: Optional[str] = None,
        scopes: Optional[Sequence[str]] = None,
        redirect_uri: Optional[str] = None,
        allowed_emails: Sequence[str] = (),
        allowed_domains: Sequence[str] = (),
        allowed_users: Sequence[str] = (),
        allow_any_user: bool = False,
        secret_key: Optional[str] = None,
        session_lifetime: float = 8 * 3600,
        idle_timeout: Optional[float] = DEFAULT_IDLE_TIMEOUT,
        timeout: float = 10.0,
    ):
        provider = provider.lower()
        if provider not in PROVIDERS:
            raise ValueError(f"Unknown OAuth provider {provider!r}; use one of {', '.join(PROVIDERS)}")
        if not client_id:
            raise ValueError("OAuth: client_id is required")
        preset = PROVIDERS[provider]
        self.provider = provider
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant = tenant or preset.get("tenant")
        issuer = issuer or preset.get("issuer")
        self.issuer = issuer.format(tenant=self.tenant) if issuer else None
        self.authorize_url = authorize_url or preset.get("authorize_url")
        self.token_url = token_url or preset.get("token_url")
        self.userinfo_url = userinfo_url or preset.get("userinfo_url")
        self.emails_url = preset.get("emails_url")
        if not self.issuer and not (self.authorize_url and self.token_url and self.userinfo_url):
            raise ValueError("OAuth: give `issuer` (OpenID Connect discovery) or authorize_url, token_url "
                             "and userinfo_url")
        self.scopes = list(scopes or preset["scopes"])
        self.authorize_params = dict(preset.get("authorize_params", {}))
        self.redirect_uri = redirect_uri
        self.allowed_emails = {e.strip().lower() for e in allowed_emails if e.strip()}
        self.allowed_domains = {d.strip().lower().lstrip("@") for d in allowed_domains if d.strip()}
        self.allowed_users = {u.strip().lower() for u in allowed_users if u.strip()}
        self.allow_any_user = allow_any_user
        if not (self.allowed_emails or self.allowed_domains or self.allowed_users or allow_any_user):
            raise ValueError("OAuth: set allowed_emails, allowed_domains or allowed_users (or allow_any_user=True "
                             "to admit every account the provider accepts)")
        self.secret_key = _resolve_secret_key(secret_key, "OAuth")
        self.session_lifetime = session_lifetime
        self.idle_timeout = idle_timeout or None
        self.timeout = timeout
        self._discovered = False
        self._discovery_lock = threading.Lock()

    def describe(self) -> str:
        who = sorted(self.allowed_emails | {"@" + d for d in self.allowed_domains} | self.allowed_users)
        text = f"oauth ({self.provider}; {'any account' if self.allow_any_user else ', '.join(who)})"
        return text + f", idle timeout {format_duration(self.idle_timeout)}" if self.idle_timeout else text

    # -- endpoints ----------------------------------------------------------------

    def _endpoints(self) -> None:
        if self._discovered or not self.issuer:
            return
        with self._discovery_lock:
            if self._discovered:
                return
            meta = _http_json(self.issuer.rstrip("/") + "/.well-known/openid-configuration", timeout=self.timeout)
            self.authorize_url = self.authorize_url or meta["authorization_endpoint"]
            self.token_url = self.token_url or meta["token_endpoint"]
            self.userinfo_url = self.userinfo_url or meta.get("userinfo_endpoint")
            if not self.userinfo_url:
                raise OAuthError("The provider has no userinfo endpoint; set userinfo_url")
            self._discovered = True

    def _redirect_uri(self, request: Request, callback_path: str) -> str:
        return self.redirect_uri or request.absolute_url(callback_path)

    # -- session ------------------------------------------------------------------

    def _session_data(self, request: Request) -> Optional[Dict[str, Any]]:
        data = unsign(request.cookies.get(SESSION_COOKIE), self.secret_key)
        if not data or data.get("aud") != self.client_id:
            return None
        return data

    @staticmethod
    def _user_from_data(data: Dict[str, Any], provider: str) -> User:
        return User(id=data["sub"], name=data.get("name", ""), email=data.get("email"), provider=provider)

    def current_user(self, request: Request) -> Optional[User]:
        data = self._session_data(request)
        if data is None:
            return None
        if self.idle_timeout and time.time() - data.get("last_seen", 0) > self.idle_timeout:
            return None
        return self._user_from_data(data, self.provider)

    def _touch(self, request: Request, data: Dict[str, Any]) -> None:
        refreshed = dict(data, last_seen=time.time())
        remaining = max(1, int(refreshed["exp"] - time.time()))
        request.extra_headers.append(cookie_header(SESSION_COOKIE, sign(refreshed, self.secret_key), request,
                                                    max_age=remaining))

    def authenticate(self, request, login_url):
        data = self._session_data(request)
        idle = False
        if data is not None:
            idle = bool(self.idle_timeout) and time.time() - data.get("last_seen", 0) > self.idle_timeout
            if not idle:
                if request.method == "POST" or request.wants_html:
                    self._touch(request, data)
                return self._user_from_data(data, self.provider)
        if request.wants_html:
            target = request.path + ("?" + request.environ["QUERY_STRING"] if request.environ.get("QUERY_STRING") else "")
            params = {"next": target}
            if idle:
                params["reason"] = "idle"
            return "302 Found", [("Location", f"{login_url}?{urlencode(params)}")], b""
        return text_response("401 Unauthorized", "Sign-in required.")

    # -- the flow -----------------------------------------------------------------

    def login(self, request: Request, callback_path: str) -> Response:
        """Start the authorization-code flow (with PKCE) and redirect to the provider."""
        self._endpoints()
        state, verifier = secrets.token_urlsafe(32), secrets.token_urlsafe(64)
        flow = sign({"state": state, "verifier": verifier, "next": safe_next((request.query.get("next") or [""])[0]),
                     "exp": time.time() + 600}, self.secret_key)
        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": self._redirect_uri(request, callback_path),
            "scope": " ".join(self.scopes),
            "state": state,
            "code_challenge": _b64(hashlib.sha256(verifier.encode("ascii")).digest()),
            "code_challenge_method": "S256",
            **self.authorize_params,
        }
        location = self.authorize_url + ("&" if "?" in self.authorize_url else "?") + urlencode(params)
        return "302 Found", [("Location", location), cookie_header(FLOW_COOKIE, flow, request, max_age=600)], b""

    def callback(self, request: Request, callback_path: str) -> Tuple[Optional[User], Response]:
        """Finish the flow. Returns (user or None, response)."""
        clear_flow = cookie_header(FLOW_COOKIE, "", request, max_age=0)
        q = {k: v[0] for k, v in request.query.items()}
        if "error" in q:
            raise OAuthError(f"The provider refused the sign-in: {q.get('error_description') or q['error']}")
        flow = unsign(request.cookies.get(FLOW_COOKIE), self.secret_key)
        if not flow or not q.get("state") or not hmac.compare_digest(q["state"], flow.get("state", "")):
            raise OAuthError("The sign-in attempt expired or was tampered with. Please try again.")
        if not q.get("code"):
            raise OAuthError("The provider returned no authorization code.")
        self._endpoints()
        token = _http_json(self.token_url, timeout=self.timeout, data={
            "grant_type": "authorization_code",
            "code": q["code"],
            "redirect_uri": self._redirect_uri(request, callback_path),
            "client_id": self.client_id,
            **({"client_secret": self.client_secret} if self.client_secret else {}),
            "code_verifier": flow["verifier"],
        })
        access_token = token.get("access_token") if isinstance(token, dict) else None
        if not access_token:
            raise OAuthError(f"The provider returned no access token: {str(token)[:200]}")
        identity = self._identity(access_token)
        allowed, reason = self._is_allowed(identity)
        if not allowed:
            logger.warning("Dashboard sign-in denied for %s (%s): %s", identity.get("login") or identity.get("sub"),
                           self.provider, reason)
            return None, ("403 Forbidden", [clear_flow], reason.encode("utf-8"))
        user = User(id=str(identity["sub"]), name=identity.get("name") or identity.get("login") or "",
                    email=identity.get("email") if identity.get("email_trusted") else None, provider=self.provider)
        now = time.time()
        session = sign({"sub": user.id, "name": user.name, "email": user.email, "aud": self.client_id,
                        "exp": now + self.session_lifetime, "last_seen": now}, self.secret_key)
        logger.info("Dashboard sign-in: %s (%s)", user.display, self.provider)
        return user, ("303 See Other", [("Location", request.base + flow["next"]), clear_flow,
                                        cookie_header(SESSION_COOKIE, session, request,
                                                      max_age=int(self.session_lifetime))], b"")

    # -- identity & authorization ----------------------------------------------------

    def _identity(self, access_token: str) -> Dict[str, Any]:
        info = _http_json(self.userinfo_url, token=access_token, timeout=self.timeout)
        if not isinstance(info, dict):
            raise OAuthError("Unexpected userinfo response")
        if self.provider == "github":
            email = None
            if self.emails_url:
                emails = _http_json(self.emails_url, token=access_token, timeout=self.timeout)
                primary = [e for e in emails if isinstance(e, dict) and e.get("primary") and e.get("verified")]
                email = primary[0]["email"] if primary else None
            return {"sub": str(info["id"]), "login": info.get("login"), "name": info.get("name") or info.get("login"),
                    "email": email, "email_trusted": email is not None}
        email = info.get("email")
        if self.provider == "microsoft":
            # Microsoft doesn't vouch for the email claim in multi-tenant apps (it is user-editable
            # in some tenants), so only trust it for a single, specific tenant.
            trusted = bool(email) and (self.tenant or "common").lower() not in _MULTI_TENANT
        else:
            trusted = bool(email) and info.get("email_verified") in (True, "true")
        return {"sub": info.get("sub") or info.get("oid") or info.get("id"), "login": info.get("preferred_username"),
                "name": info.get("name"), "email": email, "email_trusted": trusted}

    def _is_allowed(self, identity: Dict[str, Any]) -> Tuple[bool, str]:
        if not identity.get("sub"):
            return False, "The provider did not identify the user."
        if self.allow_any_user:
            return True, ""
        names = {str(identity.get(k) or "").lower() for k in ("sub", "login")} - {""}
        if names & self.allowed_users:
            return True, ""
        email = (identity.get("email") or "").lower()
        if email and identity.get("email_trusted"):
            if email in self.allowed_emails or email.rpartition("@")[2] in self.allowed_domains:
                return True, ""
        elif email and (self.allowed_emails or self.allowed_domains):
            return False, "Your account's email address is not verified by the provider, so it can't be used here."
        return False, "Your account is not allowed to use this dashboard."


def resolve_auth(auth: Union[None, str, DashboardAuth], basic_auth: Optional[Tuple[str, str]] = None) -> DashboardAuth:
    if basic_auth is not None:
        return BasicAuth(username=basic_auth[0], password=basic_auth[1])
    if auth is None or auth == "none":
        return NoAuth()
    if auth == "local":
        return LocalOnlyAuth()
    if isinstance(auth, DashboardAuth):
        return auth
    raise ValueError(f"auth must be 'none', 'local', or a BasicAuth / OAuth instance, got {auth!r}")

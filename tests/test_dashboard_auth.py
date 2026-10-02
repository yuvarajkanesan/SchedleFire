"""Dashboard authentication (none / local / basic / OAuth) and custom routing."""

import base64
import hashlib
import io
import json
import os
import secrets
import threading
import time
import unittest
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from urllib.parse import parse_qs, urlencode, urlsplit

from schedulefire import BackgroundJobClient, MemoryStorage, RecurringJobManager
from schedulefire.dashboard import BasicAuth, DashboardApp, OAuth, hash_password, run_dashboard, verify_password
from schedulefire.dashboard.auth import OAuthError, _http_json
from schedulefire.settings import ConfigError, parse_settings
from tests import jobs

SECRET = "x" * 40


# ------------------------------------------------------------------ a fake OpenID Connect provider


class FakeProvider:
    """OIDC discovery, authorization codes with PKCE, token and userinfo endpoints."""

    def __init__(self):
        self.claims = {"sub": "u-1", "email": "ada@example.com", "email_verified": True, "name": "Ada Lovelace"}
        self.github_emails = None
        self.codes, self.tokens = {}, {}
        provider = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def send_json(self, data, status=200):
                body = json.dumps(data).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                if self.path == "/.well-known/openid-configuration":
                    base = provider.base
                    return self.send_json({"issuer": base, "authorization_endpoint": base + "/authorize",
                                           "token_endpoint": base + "/token", "userinfo_endpoint": base + "/userinfo"})
                token = self.headers.get("Authorization", "").replace("Bearer ", "")
                if token not in provider.tokens:
                    return self.send_json({"error": "invalid_token"}, 401)
                if self.path == "/userinfo":
                    return self.send_json(provider.tokens[token])
                if self.path == "/emails":
                    return self.send_json(provider.github_emails or [])
                self.send_json({"error": "not found"}, 404)

            def do_POST(self):
                form = {k: v[0] for k, v in parse_qs(self.rfile.read(int(self.headers["Content-Length"])).decode()).items()}
                grant = provider.codes.pop(form.get("code"), None)
                challenge = base64.urlsafe_b64encode(
                    hashlib.sha256(form.get("code_verifier", "").encode()).digest()).rstrip(b"=").decode()
                if (grant is None or form.get("client_secret") != "shh" or form.get("client_id") != "dash"
                        or challenge != grant["code_challenge"] or form.get("redirect_uri") != grant["redirect_uri"]):
                    return self.send_json({"error": "invalid_grant"}, 400)
                token = secrets.token_urlsafe(16)
                provider.tokens[token] = dict(provider.claims)
                self.send_json({"access_token": token, "token_type": "Bearer"})

        class Server(ThreadingMixIn, HTTPServer):
            daemon_threads = True

        self.httpd = Server(("127.0.0.1", 0), Handler)
        self.base = f"http://127.0.0.1:{self.httpd.server_port}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def approve(self, authorize_url: str) -> str:
        """What the provider does after the user signs in: issue a code for these parameters."""
        params = {k: v[0] for k, v in parse_qs(urlsplit(authorize_url).query).items()}
        code = secrets.token_urlsafe(16)
        self.codes[code] = params
        return code

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


class Browser:
    """Calls the WSGI app like a browser: keeps cookies between requests."""

    def __init__(self, app, host="dash.test", remote="10.1.1.1"):
        self.app, self.host, self.remote = app, host, remote
        self.cookies = {}

    def request(self, url, method="GET", body="", **headers):
        path, _, query = url.partition("?")
        environ = {"REQUEST_METHOD": method, "PATH_INFO": path, "QUERY_STRING": query, "SCRIPT_NAME": "",
                   "REMOTE_ADDR": self.remote, "HTTP_HOST": self.host, "SERVER_NAME": self.host, "SERVER_PORT": "80",
                   "wsgi.url_scheme": "http", "CONTENT_LENGTH": str(len(body)), "wsgi.input": io.BytesIO(body.encode()),
                   "HTTP_COOKIE": "; ".join(f"{k}={v}" for k, v in self.cookies.items())}
        environ.update(headers)
        out = {}
        chunks = self.app(environ, lambda status, hdrs: out.update(status=status, headers=hdrs))
        for name, value in out["headers"]:
            if name == "Set-Cookie":
                pair = value.split(";")[0]
                key, _, val = pair.partition("=")
                if "Max-Age=0" in value:
                    self.cookies.pop(key, None)
                else:
                    self.cookies[key] = val
        out["body"] = b"".join(chunks).decode("utf-8", "replace")
        out["location"] = dict(out["headers"]).get("Location", "")
        return out

    def csrf(self):
        return self.cookies["schedulefire_csrf"]


# ------------------------------------------------------------------ tests


class OpenDashboardTests(unittest.TestCase):
    def test_no_auth_is_the_default(self):
        app = DashboardApp(MemoryStorage())
        self.assertEqual(app.auth.name, "none")
        with self.assertLogs("schedulefire.dashboard", "WARNING") as logs:
            self.assertEqual(Browser(app).request("/")["status"], "200 OK")
        self.assertIn("WITHOUT authentication", logs.output[0])


class BasicAuthTests(unittest.TestCase):
    def basic(self, user, password):
        return {"HTTP_AUTHORIZATION": "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()}

    def test_hashing(self):
        hashed = hash_password("correct horse")
        self.assertTrue(hashed.startswith("pbkdf2_sha256$600000$"))
        self.assertTrue(verify_password("correct horse", hashed))
        self.assertFalse(verify_password("wrong", hashed))
        self.assertNotEqual(hash_password("same"), hash_password("same"))  # salted
        self.assertFalse(verify_password("x", "pbkdf2_sha256$broken"))

    def test_several_users_plain_and_hashed(self):
        app = DashboardApp(MemoryStorage(), auth=BasicAuth({"admin": hash_password("s3cret!"), "ops": "plain-pw"}))
        b = Browser(app)
        # A plain browser navigation with no credentials gets a proper sign-in form, not the
        # browser's native Basic-auth popup - see BasicLoginFormTests for that flow.
        self.assertEqual(b.request("/")["status"], "302 Found")
        self.assertEqual(b.request("/", **self.basic("admin", "s3cret!"))["status"], "200 OK")
        self.assertIn("admin", b.request("/", **self.basic("admin", "s3cret!"))["body"])  # shown as the user
        self.assertEqual(b.request("/", **self.basic("ops", "plain-pw"))["status"], "200 OK")
        self.assertEqual(b.request("/", **self.basic("ghost", "s3cret!"))["status"], "401 Unauthorized")
        with self.assertRaises(ValueError):
            BasicAuth({})


class BasicLoginFormTests(unittest.TestCase):
    """The proper HTML sign-in form + session cookie + idle timeout, as distinct from the
    stateless Authorization-header path covered by BasicAuthTests."""

    def setUp(self):
        self.storage = MemoryStorage()
        self.job_id = BackgroundJobClient(self.storage).enqueue(jobs.add, 1, 2)

    def app(self, **kw):
        kw.setdefault("secret_key", SECRET)
        return DashboardApp(self.storage, auth=BasicAuth(username="admin", password="s3cret!", **kw))

    def sign_in(self, b, password="s3cret!", next_path="/"):
        login = b.request(f"/auth/login?next={next_path}")
        self.assertEqual(login["status"], "200 OK")
        self.assertIn('<form method="post" action="/auth/login">', login["body"])
        return b.request("/auth/login", "POST", f"username=admin&password={password}&csrf={b.csrf()}&next={next_path}",
                         HTTP_ORIGIN="http://dash.test")

    def test_full_sign_in_and_sign_out(self):
        app = self.app()
        b = Browser(app)
        start = b.request(f"/job/{self.job_id}")
        self.assertEqual(start["status"], "302 Found")
        self.assertTrue(start["location"].startswith("/auth/login?next="))

        bad = self.sign_in(b, password="wrong")
        self.assertEqual(bad["status"], "401 Unauthorized")
        self.assertIn("Incorrect username or password", bad["body"])
        self.assertNotIn("schedulefire_session", b.cookies)

        ok = self.sign_in(b, next_path=f"/job/{self.job_id}")
        self.assertEqual(ok["status"], "303 See Other")
        self.assertEqual(ok["location"], f"/job/{self.job_id}")
        self.assertIn("schedulefire_session", b.cookies)

        page = b.request(f"/job/{self.job_id}")
        self.assertEqual(page["status"], "200 OK")
        self.assertIn(">admin<", page["body"])
        self.assertIn("Sign out", page["body"])

        out = b.request("/auth/logout", "POST", f"csrf={b.csrf()}", HTTP_ORIGIN="http://dash.test")
        self.assertEqual(out["status"], "200 OK")
        self.assertIn("signed out", out["body"])
        self.assertNotIn("schedulefire_session", b.cookies)  # regression test: touch-on-POST must not undo this
        self.assertEqual(b.request("/")["status"], "302 Found")

    def test_idle_timeout_signs_out_and_shows_why(self):
        b = Browser(self.app(idle_timeout=0.3))
        self.sign_in(b)
        self.assertEqual(b.request("/")["status"], "200 OK")
        time.sleep(0.4)
        expired = b.request("/")
        self.assertEqual(expired["status"], "302 Found")
        self.assertIn("reason=idle", expired["location"])
        login = b.request(expired["location"])
        self.assertIn("of inactivity", login["body"])

    def test_staying_active_extends_the_session_past_the_idle_window(self):
        b = Browser(self.app(idle_timeout=0.3))
        self.sign_in(b)
        for _ in range(4):
            time.sleep(0.15)
            self.assertEqual(b.request("/")["status"], "200 OK")  # each real navigation touches/refreshes

    def test_background_live_refresh_does_not_count_as_activity(self):
        b = Browser(self.app(idle_timeout=0.3))
        self.sign_in(b)
        time.sleep(0.4)
        # A background poll (marked, like the dashboard's own live-refresh) must not be
        # silently kept alive by polling alone, and gets 401 (not a redirect) once idle.
        live = b.request("/", HTTP_X_REQUESTED_WITH="schedulefire-live")
        self.assertEqual(live["status"], "401 Unauthorized")

    def test_stateless_header_auth_ignores_idle_timeout_entirely(self):
        app = self.app(idle_timeout=0.2)
        b = Browser(app)
        auth = {"HTTP_AUTHORIZATION": "Basic " + base64.b64encode(b"admin:s3cret!").decode()}
        self.assertEqual(b.request("/", **auth)["status"], "200 OK")
        time.sleep(0.3)
        self.assertEqual(b.request("/", **auth)["status"], "200 OK")  # no session was ever created
        self.assertNotIn("schedulefire_session", b.cookies)

    def test_login_form_is_throttled_like_the_header_path(self):
        b = Browser(self.app())
        for _ in range(10):
            self.sign_in(b, password="wrong")
        blocked = self.sign_in(b, password="s3cret!")
        self.assertEqual(blocked["status"], "429 Too Many Requests")

    def test_login_requires_csrf_and_same_origin(self):
        b = Browser(self.app())
        b.request("/auth/login")  # sets the csrf cookie
        no_token = b.request("/auth/login", "POST", "username=admin&password=s3cret!&next=/",
                             HTTP_ORIGIN="http://dash.test")
        self.assertEqual(no_token["status"], "403 Forbidden")
        cross_site = b.request("/auth/login", "POST", f"username=admin&password=s3cret!&csrf={b.csrf()}&next=/",
                               HTTP_ORIGIN="https://evil.example")
        self.assertEqual(cross_site["status"], "403 Forbidden")
        self.assertNotIn("schedulefire_session", b.cookies)

    def test_next_parameter_cannot_redirect_off_site(self):
        b = Browser(self.app())
        for target in ("//evil.example", "https://evil.example"):
            ok = self.sign_in(b, next_path=target)
            self.assertEqual(ok["location"], "/")

    def test_removed_user_invalidates_an_existing_session(self):
        app = self.app()
        b = Browser(app)
        self.sign_in(b)
        self.assertEqual(b.request("/")["status"], "200 OK")
        app.auth.users.pop("admin")
        gone = b.request("/")
        self.assertEqual(gone["status"], "302 Found")


class OAuthTests(unittest.TestCase):
    def setUp(self):
        self.provider = FakeProvider()
        self.addCleanup(self.provider.close)
        self.storage = MemoryStorage()
        self.job_id = BackgroundJobClient(self.storage).enqueue(jobs.add, 1, 2)

    def oauth(self, **overrides):
        options = dict(client_id="dash", client_secret="shh", issuer=self.provider.base,
                       allowed_domains=["example.com"], secret_key=SECRET)
        options.update(overrides)
        return OAuth("oidc", **options)

    def sign_in(self, browser, start="/jobs/failed"):
        first = browser.request(start)
        self.assertEqual(first["status"], "302 Found")
        self.assertTrue(first["location"].startswith("/auth/login?next="))
        to_provider = browser.request(first["location"])
        self.assertEqual(to_provider["status"], "302 Found")
        authorize = to_provider["location"]
        params = {k: v[0] for k, v in parse_qs(urlsplit(authorize).query).items()}
        self.assertTrue(authorize.startswith(self.provider.base + "/authorize"))
        self.assertEqual((params["response_type"], params["code_challenge_method"]), ("code", "S256"))
        self.assertEqual(params["redirect_uri"], "http://dash.test/auth/callback")
        code = self.provider.approve(authorize)
        return browser.request(f"/auth/callback?{urlencode({'code': code, 'state': params['state']})}"), params

    def test_full_sign_in_flow(self):
        app = DashboardApp(self.storage, auth=self.oauth())
        b = Browser(app)
        done, _ = self.sign_in(b)
        self.assertEqual(done["status"], "303 See Other")
        self.assertEqual(done["location"], "/jobs/failed")
        self.assertIn("schedulefire_session", b.cookies)
        page = b.request("/jobs/failed")
        self.assertEqual(page["status"], "200 OK")
        self.assertIn("Ada Lovelace", page["body"])
        self.assertIn("Sign out", page["body"])
        # a background refresh without a session gets 401, not a redirect
        self.assertEqual(Browser(app).request("/", HTTP_X_REQUESTED_WITH="fetch")["status"], "401 Unauthorized")

    def test_sessions_work_across_processes_with_the_same_key(self):
        b = Browser(DashboardApp(self.storage, auth=self.oauth()))
        self.sign_in(b)
        b.app = DashboardApp(self.storage, auth=self.oauth())  # "another process", same secret_key
        self.assertEqual(b.request("/")["status"], "200 OK")
        b.app = DashboardApp(self.storage, auth=self.oauth(secret_key="y" * 40))
        self.assertEqual(b.request("/")["status"], "302 Found")

    def test_idle_timeout(self):
        auth = self.oauth(idle_timeout=0.3, session_lifetime=8 * 3600)  # idle fires well before the absolute cap
        b = Browser(DashboardApp(self.storage, auth=auth))
        self.sign_in(b)
        self.assertEqual(b.request("/")["status"], "200 OK")
        time.sleep(0.4)
        expired = b.request("/")
        self.assertEqual(expired["status"], "302 Found")
        self.assertIn("reason=idle", expired["location"])

    def test_staying_active_extends_the_session_past_the_idle_window(self):
        b = Browser(DashboardApp(self.storage, auth=self.oauth(idle_timeout=0.3)))
        self.sign_in(b)
        for _ in range(4):
            time.sleep(0.15)
            self.assertEqual(b.request("/")["status"], "200 OK")

    def test_background_poll_does_not_extend_an_oauth_session(self):
        b = Browser(DashboardApp(self.storage, auth=self.oauth(idle_timeout=0.3)))
        self.sign_in(b)
        time.sleep(0.4)
        self.assertEqual(b.request("/", HTTP_X_REQUESTED_WITH="schedulefire-live")["status"], "401 Unauthorized")

    def test_idle_timeout_disabled(self):
        b = Browser(DashboardApp(self.storage, auth=self.oauth(idle_timeout=None)))
        self.sign_in(b)
        time.sleep(0.3)
        self.assertEqual(b.request("/")["status"], "200 OK")  # no idle check at all

    def test_tampered_and_expired_sessions_are_rejected(self):
        auth = self.oauth(session_lifetime=1)
        b = Browser(DashboardApp(self.storage, auth=auth))
        self.sign_in(b)
        payload, _, sig = b.cookies["schedulefire_session"].rpartition(".")
        forged = json.loads(base64.urlsafe_b64decode(payload + "=="))
        forged["name"] = "Mallory"
        good = b.cookies["schedulefire_session"]
        b.cookies["schedulefire_session"] = base64.urlsafe_b64encode(json.dumps(forged).encode()).rstrip(b"=").decode() + "." + sig
        self.assertEqual(b.request("/")["status"], "302 Found")
        b.cookies["schedulefire_session"] = good
        time.sleep(1.2)
        self.assertEqual(b.request("/")["status"], "302 Found")

    def test_state_must_match(self):
        b = Browser(DashboardApp(self.storage, auth=self.oauth()))
        b.request(b.request("/")["location"])
        bad = b.request("/auth/callback?code=abc&state=forged")
        self.assertEqual(bad["status"], "400 Bad Request")
        self.assertIn("expired or was tampered", bad["body"])
        fresh = Browser(b.app)  # no flow cookie at all
        self.assertEqual(fresh.request("/auth/callback?code=abc&state=x")["status"], "400 Bad Request")

    def test_pkce_verifier_is_enforced_by_the_token_endpoint(self):
        b = Browser(DashboardApp(self.storage, auth=self.oauth()))
        b.request(b.request("/")["location"])
        stolen = self.provider.approve(self.provider.base + "/authorize?" + urlencode(
            {"code_challenge": "attacker-challenge", "redirect_uri": "http://dash.test/auth/callback"}))
        flow_state = json.loads(base64.urlsafe_b64decode(b.cookies["schedulefire_oauth"].rpartition(".")[0] + "=="))
        res = b.request(f"/auth/callback?{urlencode({'code': stolen, 'state': flow_state['state']})}")
        self.assertEqual(res["status"], "400 Bad Request")

    def test_only_allowed_users_get_in(self):
        self.provider.claims = {"sub": "u-2", "email": "eve@evil.example", "email_verified": True, "name": "Eve"}
        b = Browser(DashboardApp(self.storage, auth=self.oauth()))
        denied, _ = self.sign_in(b)
        self.assertEqual(denied["status"], "403 Forbidden")
        self.assertIn("not allowed", denied["body"])
        self.assertNotIn("schedulefire_session", b.cookies)
        # an unverified email can't satisfy a domain rule
        self.provider.claims = {"sub": "u-3", "email": "ada@example.com", "email_verified": False}
        denied, _ = self.sign_in(Browser(b.app))
        self.assertEqual(denied["status"], "403 Forbidden")
        self.assertIn("not verified", denied["body"])
        # allowed_users matches the subject / username
        self.provider.claims = {"sub": "robot-7", "preferred_username": "ci-bot"}
        ok, _ = self.sign_in(Browser(DashboardApp(self.storage, auth=self.oauth(allowed_domains=[],
                                                                                allowed_users=["ci-bot"]))))
        self.assertEqual(ok["status"], "303 See Other")

    def test_sign_out(self):
        b = Browser(DashboardApp(self.storage, auth=self.oauth()))
        self.sign_in(b)
        self.assertIn("Sign out", b.request("/")["body"])  # the page with the button sets the CSRF cookie
        out = b.request("/auth/logout", "POST", f"csrf={b.csrf()}", HTTP_ORIGIN="http://dash.test")
        self.assertEqual(out["status"], "200 OK")
        self.assertIn("signed out", out["body"])
        self.assertNotIn("schedulefire_session", b.cookies)
        self.assertEqual(b.request("/")["status"], "302 Found")

    def test_next_parameter_cannot_redirect_off_site(self):
        b = Browser(DashboardApp(self.storage, auth=self.oauth()))
        to_provider = b.request("/auth/login?next=//evil.example/x")
        code = self.provider.approve(to_provider["location"])
        state = parse_qs(urlsplit(to_provider["location"]).query)["state"][0]
        done = b.request(f"/auth/callback?{urlencode({'code': code, 'state': state})}")
        self.assertEqual(done["location"], "/")

    def test_github_style_provider(self):
        base = self.provider.base
        self.provider.claims = {"id": 42, "login": "octocat", "name": "The Octocat"}
        self.provider.github_emails = [{"email": "octo@example.com", "primary": True, "verified": True}]
        auth = OAuth("github", client_id="dash", client_secret="shh", authorize_url=base + "/authorize",
                     token_url=base + "/token", userinfo_url=base + "/userinfo", allowed_users=["octocat"],
                     secret_key=SECRET)
        auth.emails_url = base + "/emails"
        b = Browser(DashboardApp(self.storage, auth=auth))
        done, params = self.sign_in(b)
        self.assertEqual(done["status"], "303 See Other")
        self.assertEqual(params["scope"], "read:user user:email")
        self.assertIn("The Octocat", b.request("/")["body"])

    def test_configuration_guards(self):
        with self.assertRaises(ValueError):
            OAuth("google", client_id="x", secret_key=SECRET)  # no allowlist
        with self.assertRaises(ValueError):
            OAuth("google", client_id="x", allowed_domains=["a.com"], secret_key="short")
        with self.assertRaises(ValueError):
            OAuth("oidc", client_id="x", allowed_domains=["a.com"], secret_key=SECRET)  # no issuer
        with self.assertRaises(ValueError):
            OAuth("myspace", client_id="x", allow_any_user=True, secret_key=SECRET)
        with self.assertRaises(OAuthError):
            _http_json("http://idp.example.com/token")  # plain http only allowed for localhost
        ms = OAuth("microsoft", client_id="x", tenant="contoso.onmicrosoft.com", allowed_domains=["contoso.com"],
                   secret_key=SECRET)
        self.assertEqual(ms.issuer, "https://login.microsoftonline.com/contoso.onmicrosoft.com/v2.0")


class RoutingTests(unittest.TestCase):
    def setUp(self):
        self.storage = MemoryStorage()
        self.job_id = BackgroundJobClient(self.storage).enqueue(jobs.add, 1, 2)
        RecurringJobManager(self.storage).add_or_update("r1", jobs.add, "@daily")

    def test_custom_routes(self):
        app = DashboardApp(self.storage, routes={"recurring": "/schedules", "job": "/tasks/{id}",
                                                 "jobs": "/list/{state}", "servers": None})
        b = Browser(app)
        home = b.request("/")["body"]
        self.assertIn('href="/schedules"', home)
        self.assertIn('href="/list/failed"', home)
        self.assertNotIn('href="/servers"', home)
        self.assertEqual(b.request("/schedules")["status"], "200 OK")
        self.assertEqual(b.request("/recurring")["status"], "404 Not Found")   # old path is gone
        self.assertEqual(b.request("/servers")["status"], "404 Not Found")     # disabled
        enqueued = b.request("/list/enqueued")
        self.assertIn(f'href="/tasks/{self.job_id}"', enqueued["body"])
        self.assertIn("add(1, 2)", b.request(f"/tasks/{self.job_id}")["body"])
        done = b.request("/actions/jobs/delete", "POST", f"id={self.job_id}&next=/jobs/deleted&csrf={b.csrf()}")
        self.assertTrue(done["location"].startswith("/list/deleted?notice="))

    def test_route_validation(self):
        bad = [{"nope": "/x"}, {"job": "/job"}, {"jobs": "/jobs/{id}"}, {"overview": None}, {"servers": "servers"},
               {"recurring": "/assets/r"}, {"recurring": "/same", "queues": "/same"}]
        for routes in bad:
            with self.subTest(routes=routes), self.assertRaises(ValueError):
                DashboardApp(self.storage, routes=routes)

    def test_custom_pages(self):
        app = DashboardApp(self.storage, auth=BasicAuth(username="admin", password="pw"))

        @app.page("/stats", "Team stats", icon="servers")
        def stats(ctx):
            count = ctx.storage.count_jobs("Enqueued")
            return f"<p>Waiting: {count}</p><p>{ctx.escape('<b>')}</p><a href=\"{ctx.url('/jobs/failed')}\">x</a>"

        b = Browser(app)
        self.assertEqual(b.request("/stats")["status"], "302 Found")  # protected like every page
        auth = {"HTTP_AUTHORIZATION": "Basic " + base64.b64encode(b"admin:pw").decode()}
        page = b.request("/stats", **auth)
        self.assertEqual(page["status"], "200 OK")
        self.assertIn("Waiting: 1", page["body"])
        self.assertIn("&lt;b&gt;", page["body"])
        self.assertIn('href="/stats"', b.request("/", **auth)["body"])  # in the sidebar
        with self.assertRaises(ValueError):
            app.add_page("/recurring", "Clash", lambda ctx: "")

    def test_served_under_a_path(self):
        httpd = run_dashboard(self.storage, port=0, path="/admin/jobs", background=True,
                              routes={"recurring": "/schedules"})
        self.addCleanup(httpd.server_close)
        self.addCleanup(httpd.shutdown)
        base = f"http://127.0.0.1:{httpd.server_port}"

        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args):
                return None

        opener = urllib.request.build_opener(NoRedirect)
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            opener.open(base + "/")
        self.assertEqual((ctx.exception.code, ctx.exception.headers["Location"]), (302, "/admin/jobs/"))
        ctx.exception.close()
        with urllib.request.urlopen(base + "/admin/jobs/") as resp:
            html = resp.read().decode()
        self.assertIn('href="/admin/jobs/schedules"', html)
        self.assertIn('href="/admin/jobs/assets/app.css', html)
        with urllib.request.urlopen(base + "/admin/jobs/schedules") as resp:
            self.assertEqual(resp.status, 200)

    def test_serves_a_prebuilt_dashboard_app(self):
        # A DashboardApp configured beforehand (custom pages added, ...) must be served as-is,
        # not discarded in favor of a fresh one built from `storage`.
        app = DashboardApp(self.storage)

        @app.page("/stats", "Team stats")
        def stats(ctx):
            return "<p>hello from a pre-built app</p>"

        httpd = run_dashboard(MemoryStorage(), port=0, background=True, dashboard=app)
        self.addCleanup(httpd.server_close)
        self.addCleanup(httpd.shutdown)
        base = f"http://127.0.0.1:{httpd.server_port}"
        with urllib.request.urlopen(base + "/schedule-fire/stats") as resp:
            html = resp.read().decode()
        self.assertIn("hello from a pre-built app", html)
        self.assertNotIn("No jobs registered", html)


class DefaultPathTests(unittest.TestCase):
    """The dashboard lives under /schedule-fire so it never collides with the host app's routes."""

    def test_mounted_dashboard_leaves_the_app_alone(self):
        from schedulefire.dashboard import DEFAULT_PATH, mount_dashboard

        self.assertEqual(DEFAULT_PATH, "/schedule-fire")
        seen = []

        def host_app(environ, start_response):
            seen.append(environ["PATH_INFO"])
            start_response("200 OK", [("Content-Type", "text/plain")])
            return [b"host"]

        app = mount_dashboard(host_app, storage=MemoryStorage())
        for path in ("/", "/jobs", "/jobs/failed", "/admin", "/schedule-firework"):
            self.assertEqual(Browser(app).request(path)["body"], "host", path)
        self.assertEqual(seen, ["/", "/jobs", "/jobs/failed", "/admin", "/schedule-firework"])
        page = Browser(app).request("/schedule-fire/jobs/failed")
        self.assertEqual(page["status"], "200 OK")
        self.assertIn('href="/schedule-fire/recurring"', page["body"])

    def test_standalone_default_path(self):
        httpd = run_dashboard(MemoryStorage(), port=0, background=True)
        self.addCleanup(httpd.server_close)
        self.addCleanup(httpd.shutdown)
        base = f"http://127.0.0.1:{httpd.server_port}"

        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *args):
                return None

        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.build_opener(NoRedirect).open(base + "/")
        self.assertEqual(ctx.exception.headers["Location"], "/schedule-fire/")
        ctx.exception.close()
        with urllib.request.urlopen(base + "/schedule-fire/") as resp:
            self.assertIn('href="/schedule-fire/servers"', resp.read().decode())
        self.assertEqual(parse_settings({}).dashboard.path, "/schedule-fire")
        root = run_dashboard(MemoryStorage(), port=0, background=True, path="/")   # opt back into the root
        self.addCleanup(root.server_close)
        self.addCleanup(root.shutdown)
        with urllib.request.urlopen(f"http://127.0.0.1:{root.server_port}/") as resp:
            self.assertEqual(resp.status, 200)


class DashboardConfigTests(unittest.TestCase):
    def test_modes_are_inferred_and_validated(self):
        self.assertEqual(parse_settings({}).dashboard.auth_mode, "none")
        basic = parse_settings({"dashboard": {"users": {"admin": hash_password("pw123456")}}})
        self.assertEqual(basic.dashboard.auth_mode, "basic")
        self.assertIsInstance(basic.dashboard.create_auth(), BasicAuth)
        oauth = parse_settings({"dashboard": {"path": "/admin/jobs", "oauth": {
            "provider": "google", "client_id": "id", "client_secret": "s", "secret_key": SECRET,
            "allowed_domains": "example.com", "session_lifetime": "2h"},
            "routes": {"servers": False, "recurring": "/schedules"}}})
        d = oauth.dashboard
        self.assertEqual((d.auth_mode, d.path, d.routes), ("oauth", "/admin/jobs", {"servers": None, "recurring": "/schedules"}))
        self.assertEqual(d.create_auth().session_lifetime, 7200)
        self.assertIn("auth oauth", oauth.describe())
        errors = [
            {"dashboard": {"auth": "magic"}},
            {"dashboard": {"auth": "basic"}},
            {"dashboard": {"auth": "oauth"}},
            {"dashboard": {"oauth": {"provider": "google", "client_id": "x"}}},             # no allowlist
            {"dashboard": {"oauth": {"provider": "google", "client_id": "x", "allow_any_user": True,
                                     "secret_key": "short"}}},
            {"dashboard": {"routes": {"overview": False}}},
            {"dashboard": {"path": "admin"}},
            {"dashboard": {"oauth": {"provider": "google", "client_id": "x", "clientsecret": "y"}}},
        ]
        for raw in errors:
            with self.subTest(raw=raw), self.assertRaises(ConfigError):
                parse_settings(raw)

    def test_idle_timeout_and_secret_key(self):
        default = parse_settings({})
        self.assertEqual(default.dashboard.idle_timeout, 300.0)  # 5 minutes by default

        basic = parse_settings({"dashboard": {"idle_timeout": "10m", "secret_key": SECRET,
                                              "users": {"admin": hash_password("pw123456")}}})
        auth = basic.dashboard.create_auth()
        self.assertIsInstance(auth, BasicAuth)
        self.assertEqual(auth.idle_timeout, 600.0)
        self.assertEqual(auth.secret_key, hashlib.sha256(SECRET.encode()).digest())

        off = parse_settings({"dashboard": {"idle_timeout": 0, "users": {"admin": "x" * 8}}})
        self.assertIsNone(off.dashboard.idle_timeout)
        self.assertIsNone(off.dashboard.create_auth().idle_timeout)

        # the top-level idle_timeout feeds oauth unless it sets its own
        inherited = parse_settings({"dashboard": {"idle_timeout": "2m", "oauth": {
            "provider": "google", "client_id": "id", "allowed_domains": "example.com", "secret_key": SECRET}}})
        self.assertEqual(inherited.dashboard.create_auth().idle_timeout, 120.0)
        overridden = parse_settings({"dashboard": {"idle_timeout": "2m", "oauth": {
            "provider": "google", "client_id": "id", "allowed_domains": "example.com", "secret_key": SECRET,
            "idle_timeout": "1h"}}})
        self.assertEqual(overridden.dashboard.create_auth().idle_timeout, 3600.0)
        self.assertIn("idle timeout 1 hour", overridden.describe())

        with self.assertRaises(ConfigError):
            parse_settings({"dashboard": {"idle_timeout": "not a duration"}})
        with self.assertRaises(ConfigError):
            parse_settings({"dashboard": {"secret_key": 12345}})


if __name__ == "__main__":
    unittest.main()

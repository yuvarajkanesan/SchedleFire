"""Dashboard routing: rename any page's URL, disable optional pages, add custom pages.

Pages are addressed internally by a fixed *canonical* path (``/job/12``); the router
translates between those and the public paths you configure, in both directions, so
every link, redirect and form in the dashboard follows your routes.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Mapping, Optional, Pattern, Tuple, Union

#: Where the dashboard lives by default - distinctive, so it never collides with your
#: own application's routes when both share a host.
DEFAULT_PATH = "/schedule-fire"

#: page name -> canonical path. Placeholders: {state} (a job state), {id} (a job id).
CANONICAL_ROUTES: Dict[str, str] = {
    "overview": "/",
    "jobs": "/jobs/{state}",
    "job": "/job/{id}",
    "recurring": "/recurring",
    "servers": "/servers",
    "queues": "/queues",
    "search": "/search",
    "login": "/auth/login",
    "callback": "/auth/callback",
    "logout": "/auth/logout",
}
#: Pages that may be switched off (route set to None / false).
OPTIONAL_PAGES = {"recurring", "servers", "queues", "search"}
#: Always served under these fixed prefixes (static files and form targets).
RESERVED_PREFIXES = ("/assets/", "/actions/")

_PLACEHOLDERS = {"state": "[a-z]+", "id": "[0-9]+"}
_TEMPLATE = re.compile(r"^/[A-Za-z0-9_\-./{}]*$")


def _compile(template: str) -> Pattern:
    pattern, pos = "", 0
    for m in re.finditer(r"\{(\w+)\}", template):
        pattern += re.escape(template[pos:m.start()]) + f"(?P<{m.group(1)}>{_PLACEHOLDERS[m.group(1)]})"
        pos = m.end()
    return re.compile("^" + pattern + re.escape(template[pos:]) + "$")


@dataclass
class CustomPage:
    path: str
    title: str
    render: Callable[..., str]
    nav: bool = True
    icon: str = "overview"


class Router:
    def __init__(self, routes: Optional[Mapping[str, Union[str, None, bool]]] = None):
        routes = dict(routes or {})
        unknown = set(routes) - set(CANONICAL_ROUTES)
        if unknown:
            raise ValueError(f"Unknown dashboard route(s): {', '.join(sorted(unknown))}; "
                             f"available: {', '.join(CANONICAL_ROUTES)}")
        self.public: Dict[str, Optional[str]] = {}
        for name, canonical in CANONICAL_ROUTES.items():
            value = routes.get(name, canonical)
            if value in (None, False):
                if name not in OPTIONAL_PAGES:
                    raise ValueError(f"The '{name}' page can't be disabled (only {', '.join(sorted(OPTIONAL_PAGES))})")
                self.public[name] = None
                continue
            if not isinstance(value, str) or not _TEMPLATE.match(value):
                raise ValueError(f"Route '{name}' must be a path starting with '/', got {value!r}")
            value = value.rstrip("/") or "/"
            wanted = set(re.findall(r"\{(\w+)\}", canonical))
            if set(re.findall(r"\{(\w+)\}", value)) != wanted:
                raise ValueError(f"Route '{name}' must contain {', '.join('{' + w + '}' for w in sorted(wanted)) or 'no placeholders'}"
                                 f" like the default {canonical!r}, got {value!r}")
            if value.startswith(RESERVED_PREFIXES) or value in ("/assets", "/actions"):
                raise ValueError(f"Route '{name}' can't use the reserved prefixes /assets and /actions")
            self.public[name] = value
        seen: Dict[str, str] = {}
        for name, value in self.public.items():
            if value is None:
                continue
            key = re.sub(r"\{\w+\}", "{}", value)
            if key in seen:
                raise ValueError(f"Routes '{seen[key]}' and '{name}' both use {value!r}")
            seen[key] = name
        self._public_patterns = [(name, _compile(p)) for name, p in self.public.items() if p is not None]
        self._canonical_patterns = [(name, _compile(p)) for name, p in CANONICAL_ROUTES.items()]
        self.pages: Dict[str, CustomPage] = {}

    def enabled(self, name: str) -> bool:
        return self.public.get(name) is not None

    def path(self, name: str, **params: Any) -> Optional[str]:
        template = self.public.get(name)
        return None if template is None else template.format(**params)

    # -- translation ----------------------------------------------------------------

    def to_canonical(self, public_path: str) -> Optional[str]:
        """Public request path -> canonical path. None means "not found"."""
        if public_path.startswith(RESERVED_PREFIXES):
            return public_path
        path = public_path.rstrip("/") or "/"
        for name, pattern in self._public_patterns:
            m = pattern.match(path)
            if m:
                return CANONICAL_ROUTES[name].format(**m.groupdict())
        if path in self.pages:
            return path
        return None

    def to_public(self, canonical: str) -> str:
        """Canonical path (with optional ?query) -> public path. Disabled pages map to themselves (404)."""
        path, sep, query = canonical.partition("?")
        if not path.startswith(RESERVED_PREFIXES) and path not in self.pages:
            for name, pattern in self._canonical_patterns:
                m = pattern.match(path)
                if m and self.public.get(name) is not None:
                    path = self.public[name].format(**m.groupdict())
                    break
        return path + sep + query

    # -- custom pages ---------------------------------------------------------------

    def add_page(self, page: CustomPage) -> None:
        if not _TEMPLATE.match(page.path) or "{" in page.path:
            raise ValueError(f"Custom page path must be a plain path starting with '/', got {page.path!r}")
        if page.path.startswith(RESERVED_PREFIXES):
            raise ValueError("Custom pages can't use the reserved prefixes /assets and /actions")
        for name, pattern in self._public_patterns:
            if pattern.match(page.path):
                raise ValueError(f"Custom page {page.path!r} collides with the '{name}' route")
        self.pages[page.path] = page

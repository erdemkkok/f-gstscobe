from __future__ import annotations

import os
import re
import time
from typing import Optional
from urllib.parse import urlsplit, urlunsplit


def now() -> float:
    return time.time()


def mono() -> float:
    return time.monotonic()


def mask_url(url: str) -> str:
    """rtsp://admin:secret@10.0.0.5/... -> rtsp://admin:***@10.0.0.5/..."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return url
    if parts.password is None:
        return url
    host = parts.hostname or ""
    if parts.port:
        host = "%s:%d" % (host, parts.port)
    netloc = "%s:***@%s" % (parts.username or "", host)
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def state_dir() -> str:
    if os.geteuid() == 0:
        return "/var/lib/gstscope"
    base = os.environ.get("XDG_STATE_HOME") or os.path.expanduser("~/.local/state")
    return os.path.join(base, "gstscope")


def web_dir() -> str:
    """Static UI files: packaged location, or ../web next to the source tree."""
    env = os.environ.get("GSTSCOPE_WEB")
    if env:
        return env
    here = os.path.dirname(os.path.abspath(__file__))
    dev = os.path.join(os.path.dirname(here), "web")
    if os.path.isfile(os.path.join(dev, "index.html")):
        return dev
    return "/usr/share/gstscope/web"


_slug_re = re.compile(r"[^a-zA-Z0-9_.-]+")


def slug(s: str) -> str:
    return _slug_re.sub("-", s).strip("-") or "x"


def fmt_rate(kbps: Optional[float]) -> str:
    if kbps is None:
        return "-"
    if kbps >= 1000:
        return "%.1f Mb/s" % (kbps / 1000.0)
    return "%.0f kb/s" % kbps


def fmt_us(us: Optional[float]) -> str:
    if us is None:
        return "-"
    if us >= 1000:
        return "%.1f ms" % (us / 1000.0)
    return "%.0f us" % us

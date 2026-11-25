"""HTTP API + Server-Sent Events + static web UI. Standard library only."""
from __future__ import annotations

import json
import logging
import mimetypes
import os
import posixpath
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional
from urllib.parse import parse_qs, unquote, urlsplit

from . import __version__
from .collector import Collector
from .prober import Prober

log = logging.getLogger("gstscope.server")

API = "/api/v1"


class App:
    def __init__(self, collector: Collector, prober: Prober, web_root: str,
                 token: Optional[str] = None, interval: float = 1.0):
        self.collector = collector
        self.prober = prober
        self.web_root = os.path.realpath(web_root)
        self.token = token
        self.interval = interval
        self.started = time.time()

    def status(self) -> dict:
        return {
            "version": __version__, "hostname": socket.gethostname(),
            "uptime": round(time.time() - self.started, 1),
            "socket": self.collector.socket_path,
            "received": self.collector.received, "bad": self.collector.bad,
            "pipelines": len(self.collector.pipelines),
            "probes": len(self.prober.probes), "board": board_name(),
        }

    def snapshot(self) -> dict:
        return {"time": round(time.time(), 3), "status": self.status(),
                "pipelines": self.collector.snapshot(), "probes": self.prober.list()}


def board_name() -> str:
    for p in ("/proc/device-tree/model", "/sys/firmware/devicetree/base/model"):
        try:
            with open(p, "rb") as f:
                return f.read().decode("utf-8", "replace").strip("\x00\n ")
        except OSError:
            continue
    return ""


class Handler(BaseHTTPRequestHandler):
    server_version = "gstscope/" + __version__
    protocol_version = "HTTP/1.1"
    app = None  # type: App

    def log_message(self, fmt: str, *args: Any) -> None:  # quieter default logging
        log.debug("%s - %s", self.address_string(), fmt % args)

    # -- helpers ---------------------------------------------------------------
    def _send(self, code: int, body: bytes, ctype: str, extra: Optional[dict] = None) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _json(self, obj: Any, code: int = 200) -> None:
        self._send(code, json.dumps(obj, separators=(",", ":")).encode(), "application/json")

    def _err(self, code: int, msg: str) -> None:
        self._json({"error": msg}, code)

    def _body(self) -> Any:
        n = int(self.headers.get("Content-Length") or 0)
        if n <= 0 or n > 1 << 20:
            return {}
        try:
            return json.loads(self.rfile.read(n).decode())
        except ValueError:
            return None

    def _authorized(self, query: dict) -> bool:
        tok = self.app.token
        if not tok:
            return True
        h = self.headers.get("Authorization", "")
        if h == "Bearer " + tok:
            return True
        return query.get("token", [""])[0] == tok

    # -- routing -----------------------------------------------------------------
    def do_HEAD(self) -> None:
        self.do_GET()

    def do_GET(self) -> None:
        url = urlsplit(self.path)
        path, query = url.path, parse_qs(url.query)
        if not path.startswith(API):
            return self._static(path)
        if not self._authorized(query):
            return self._err(401, "unauthorized")
        sub = path[len(API):].rstrip("/")
        app = self.app
        if sub == "/status":
            return self._json(app.status())
        if sub == "/snapshot":
            return self._json(app.snapshot())
        if sub == "/stream":
            return self._stream()
        if sub == "/pipelines":
            return self._json(app.collector.list())
        if sub.startswith("/pipelines/"):
            d = app.collector.get(unquote(sub.split("/", 2)[2]))
            return self._json(d) if d else self._err(404, "no such pipeline")
        if sub == "/probes":
            return self._json(app.prober.list())
        if sub.startswith("/probes/"):
            d = app.prober.get(sub.split("/", 2)[2])
            return self._json(d) if d else self._err(404, "no such probe")
        return self._err(404, "not found")

    def do_POST(self) -> None:
        url = urlsplit(self.path)
        if not self._authorized(parse_qs(url.query)):
            return self._err(401, "unauthorized")
        sub = url.path[len(API):].rstrip("/") if url.path.startswith(API) else ""
        body = self._body()
        if body is None:
            return self._err(400, "invalid json")
        if sub == "/probes":
            try:
                p = self.app.prober.add(
                    str(body.get("url", "")).strip(), str(body.get("name", "")).strip(),
                    str(body.get("transport", "tcp")), int(body.get("latency", 200)))
            except (ValueError, TypeError) as e:
                return self._err(400, str(e))
            return self._json(p.to_dict(), 201)
        if sub.startswith("/probes/") and sub.endswith("/restart"):
            pid = sub.split("/")[2]
            return self._json({"ok": True}) if self.app.prober.restart(pid) \
                else self._err(404, "no such probe")
        return self._err(404, "not found")

    def do_DELETE(self) -> None:
        url = urlsplit(self.path)
        if not self._authorized(parse_qs(url.query)):
            return self._err(401, "unauthorized")
        sub = url.path[len(API):].rstrip("/") if url.path.startswith(API) else ""
        if sub.startswith("/probes/"):
            pid = sub.split("/", 2)[2]
            return self._json({"ok": True}) if self.app.prober.remove(pid) \
                else self._err(404, "no such probe")
        return self._err(404, "not found")

    # -- SSE ---------------------------------------------------------------------
    def _stream(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "keep-alive")
        self.send_header("X-Accel-Buffering", "no")
        self.end_headers()
        self.close_connection = True
        try:
            while True:
                data = json.dumps(self.app.snapshot(), separators=(",", ":"))
                self.wfile.write(("event: snapshot\ndata: %s\n\n" % data).encode())
                self.wfile.flush()
                time.sleep(self.app.interval)
        except (BrokenPipeError, ConnectionResetError, socket.timeout, OSError):
            return

    # -- static ------------------------------------------------------------------
    def _static(self, path: str) -> None:
        if path in ("", "/"):
            path = "/index.html"
        rel = posixpath.normpath(unquote(path)).lstrip("/")
        full = os.path.realpath(os.path.join(self.app.web_root, rel))
        if not full.startswith(self.app.web_root + os.sep) or not os.path.isfile(full):
            return self._err(404, "not found")
        ctype = mimetypes.guess_type(full)[0] or "application/octet-stream"
        if ctype.startswith("text/") or ctype in ("application/javascript",):
            ctype += "; charset=utf-8"
        with open(full, "rb") as f:
            self._send(200, f.read(), ctype)


class Server:
    def __init__(self, app: App, bind: str, port: int):
        handler = type("BoundHandler", (Handler,), {"app": app})
        ThreadingHTTPServer.daemon_threads = True
        ThreadingHTTPServer.allow_reuse_address = True
        self.httpd = ThreadingHTTPServer((bind, port), handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, name="http",
                                       daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.httpd.shutdown()

"""gstscope command line."""
from __future__ import annotations

import argparse
import json
import logging
import os
import shlex
import shutil
import signal
import sys
import threading
import time
from typing import List, Optional
from urllib.error import URLError
from urllib.request import Request, urlopen

from . import DEFAULT_PORT, DEFAULT_SOCKET, __version__
from .util import fmt_rate, fmt_us, state_dir, web_dir

HELP = """\
gstscope %s - GStreamer pipeline profiler + RTSP stream checker

  gstscope serve                  start the daemon + web UI (http://<board>:%d)
  gstscope run "<gst-launch pipeline>"
  gstscope run -- ./my_app args   run any program with the tracer enabled
  gstscope env                    print the variables to trace your own service
  gstscope probe rtsp://...       one-shot stream check (no daemon needed)
  gstscope status                 show pipelines and probes from the daemon
""" % (__version__, DEFAULT_PORT)


def default_socket() -> str:
    return os.environ.get("GSTSCOPE_SOCKET") or DEFAULT_SOCKET


# ---------------------------------------------------------------------------
def cmd_serve(a: argparse.Namespace) -> int:
    from .collector import Collector
    from .prober import Prober
    from .server import App, Server

    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    log = logging.getLogger("gstscope")

    collector = Collector(a.socket)
    try:
        collector.start()
    except OSError as e:
        log.error("cannot bind %s: %s (run as root, or use --socket /tmp/gstscope.sock)",
                  a.socket, e)
        return 1

    store = os.path.join(a.state_dir or state_dir(), "probes.json")
    prober = Prober(store)
    prober.load()

    app = App(collector, prober, a.web or web_dir(), a.token or None)
    try:
        srv = Server(app, a.bind, a.port)
    except OSError as e:
        log.error("cannot listen on %s:%d: %s", a.bind, a.port, e)
        return 1
    srv.start()
    log.info("gstscope %s: http://%s:%d  (tracer socket %s)", __version__,
             a.bind if a.bind != "0.0.0.0" else "<board-ip>", a.port, a.socket)

    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    while not stop.is_set():
        stop.wait(1.0)
    log.info("shutting down")
    prober.stop_all()
    srv.stop()
    collector.stop()
    return 0


# ---------------------------------------------------------------------------
def tracer_env(socket_path: Optional[str], interval: Optional[int]) -> dict:
    env = {}
    params = []
    if socket_path:
        params.append("socket=%s" % socket_path)
    if interval:
        params.append("interval=%d" % interval)
    tr = "gstscope" + ("(%s)" % ",".join(params) if params else "")
    old = os.environ.get("GST_TRACERS", "")
    parts = [p for p in old.split(";") if p and not p.startswith("gstscope")]
    env["GST_TRACERS"] = ";".join([tr] + parts)
    return env


def cmd_env(a: argparse.Namespace) -> int:
    env = tracer_env(a.socket if a.socket != DEFAULT_SOCKET else None, a.interval)
    if a.systemd:
        print("# systemctl edit <your-service>  and add:")
        print("[Service]")
        for k, v in env.items():
            print('Environment="%s=%s"' % (k, v))
    else:
        for k, v in env.items():
            print("export %s=%s" % (k, shlex.quote(v)))
    return 0


def cmd_run(a: argparse.Namespace) -> int:
    argv: List[str] = list(a.cmd)
    if argv and argv[0] == "--":
        argv = argv[1:]
    if not argv:
        print("usage: gstscope run \"videotestsrc ! fakesink\"  |  gstscope run -- cmd args",
              file=sys.stderr)
        return 2
    if len(argv) == 1 and "!" in argv[0]:
        launch = shutil.which("gst-launch-1.0")
        if not launch:
            print("gst-launch-1.0 not found (install gstreamer1.0-tools)", file=sys.stderr)
            return 1
        argv = [launch, "-e"] + shlex.split(argv[0])
    env = dict(os.environ)
    env.update(tracer_env(a.socket if a.socket != DEFAULT_SOCKET else None, a.interval))
    if not os.path.exists(a.socket):
        print("gstscope: warning: %s does not exist - is 'gstscope serve' running?"
              % a.socket, file=sys.stderr)
    print("gstscope: GST_TRACERS=%s" % env["GST_TRACERS"], file=sys.stderr)
    try:
        os.execvpe(argv[0], argv, env)
    except OSError as e:
        print("gstscope: cannot run %s: %s" % (argv[0], e), file=sys.stderr)
        return 127
    return 0  # not reached


# ---------------------------------------------------------------------------
def _probe_report(d: dict) -> str:
    out = ["%s  [%s]%s" % (d["url"], d["state"],
                           ("  " + d["error"]) if d.get("error") else "")]
    if d.get("connect_ms") is not None:
        out.append("  first frame after %.0f ms, reconnects %d" % (d["connect_ms"],
                                                                   d["reconnects"]))
    for s in d["streams"]:
        line = "  %-5s %-7s" % (s["media"] or "?", s["codec"] or "?")
        if s.get("width"):
            line += " %dx%d" % (s["width"], s["height"])
        if s.get("profile"):
            line += " (%s)" % s["profile"]
        out.append(line)
        out.append("        fps %.2f%s  bitrate %s" % (
            s["fps"], (" (nominal %s)" % s["nominal_fps"]) if s.get("nominal_fps") else "",
            fmt_rate(s["kbps"])))
        if s["media"] == "video":
            gop = "-" if s["gop_frames"] is None else "%d frames / %.2f s" % (
                s["gop_frames"], s["gop_sec"])
            out.append("        GOP %s  keyframes %d" % (gop, s["keyframes"]))
        out.append("        jitter %.1f ms  max gap %.0f ms  ts backwards %d" % (
            s["jitter_ms"], s["max_gap_ms"], s["ts_backwards"]))
    return "\n".join(out)


def cmd_probe(a: argparse.Namespace) -> int:
    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.WARNING)
    from .prober import Prober

    prober = Prober(None)
    probes = [prober.add(u, transport=a.transport, latency=a.latency, save=False)
              for u in a.url]
    # accumulate window stats over the whole duration for the final report
    t_end = time.time() + a.duration
    totals = {p.id: {} for p in probes}  # type: dict
    t0 = time.time()
    try:
        while time.time() < t_end:
            time.sleep(0.5)
            if not a.json and sys.stderr.isatty():
                states = " ".join("%s:%s" % (p.name, p.state) for p in probes)
                sys.stderr.write("\r%4.0fs  %s   " % (time.time() - t0, states))
                sys.stderr.flush()
    except KeyboardInterrupt:
        pass
    if not a.json and sys.stderr.isatty():
        sys.stderr.write("\r" + " " * 70 + "\r")
    elapsed = max(time.time() - t0, 0.001)

    results = []
    ok = True
    for p in probes:
        d = p.to_dict()
        # replace 1 s window fps/kbps with averages over the whole run
        for s in d["streams"]:
            span = s.get("active_s") or elapsed
            s["fps"] = round(s["buffers"] / span, 2)
            s["kbps"] = round(s["bytes"] * 8 / 1000.0 / span, 1)
        if d["state"] != "playing" or not d["streams"]:
            ok = False
        results.append(d)
    prober.stop_all()
    time.sleep(0.2)

    if a.json:
        print(json.dumps(results if len(results) > 1 else results[0], indent=2))
    else:
        print("\n\n".join(_probe_report(r) for r in results))
    return 0 if ok else 1


# ---------------------------------------------------------------------------
def _get(url: str, token: Optional[str]) -> dict:
    req = Request(url)
    if token:
        req.add_header("Authorization", "Bearer " + token)
    with urlopen(req, timeout=3) as r:
        return json.loads(r.read().decode())


def cmd_status(a: argparse.Namespace) -> int:
    base = "http://%s:%d/api/v1" % (a.host, a.port)
    try:
        snap = _get(base + "/snapshot", a.token)
    except (URLError, OSError) as e:
        print("cannot reach gstscope at %s: %s" % (base, e), file=sys.stderr)
        return 1
    if a.json:
        print(json.dumps(snap, indent=2))
        return 0
    st = snap["status"]
    print("gstscope %s on %s %s  (%d msgs)" % (st["version"], st["hostname"],
                                              ("[" + st["board"] + "]") if st["board"] else "",
                                              st["received"]))
    print()
    if not snap["pipelines"]:
        print("no traced pipelines  (start one with: gstscope run \"...\")")
    for p in snap["pipelines"]:
        print("PIPELINE %s  %s pid %s  %s  [%s]" % (p["name"], p["proc"], p["pid"],
                                                    p["state"], p["status"]))
        rows = []
        for e in p["topology"]["elements"]:
            es = p["elstats"].get(e["id"], {})
            outs = [v for k, v in p["pads"].items() if v["el"] == e["id"]]
            fps = sum(o.get("fps", 0) for o in outs) if outs else None
            kbps = sum(o.get("kbps", 0) for o in outs) if outs else None
            q = es.get("queue")
            t = es.get("proc_avg_us") if "proc_avg_us" in es else es.get("sink_avg_us")
            rows.append((e["id"], e["factory"],
                         "-" if fps is None else "%.1f" % fps, fmt_rate(kbps), fmt_us(t),
                         "-" if es.get("util") is None else "%d%%" % (es["util"] * 100),
                         "%d%%" % (q["fill"] * 100) if q else ""))
        w = max([len(r[0]) for r in rows] + [7])
        print("  %-*s %-16s %8s %11s %10s %5s %5s" % (w, "element", "factory", "fps",
                                                       "bitrate", "time", "busy", "queue"))
        for r in rows:
            print("  %-*s %-16s %8s %11s %10s %5s %5s" % ((w,) + r))
        for h in p["hints"]:
            print("  ! %s" % h["text"])
        print()
    for d in snap["probes"]:
        print("PROBE " + _probe_report(d))
        print()
    return 0


# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="gstscope", description=HELP,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--version", action="version", version="gstscope " + __version__)
    sub = ap.add_subparsers(dest="command")

    s = sub.add_parser("serve", help="run daemon + web UI")
    s.add_argument("--bind", default=os.environ.get("GSTSCOPE_BIND", "0.0.0.0"))
    s.add_argument("--port", type=int, default=int(os.environ.get("GSTSCOPE_PORT",
                                                                  DEFAULT_PORT)))
    s.add_argument("--socket", default=default_socket())
    s.add_argument("--token", default=os.environ.get("GSTSCOPE_TOKEN", ""),
                   help="require this token for the API/UI (?token=...)")
    s.add_argument("--state-dir", default=os.environ.get("GSTSCOPE_STATE_DIR"))
    s.add_argument("--web", default=None, help=argparse.SUPPRESS)
    s.add_argument("-v", "--verbose", action="store_true")
    s.set_defaults(fn=cmd_serve)

    r = sub.add_parser("run", help="run a pipeline/program with the tracer")
    r.add_argument("--socket", default=default_socket())
    r.add_argument("--interval", type=int, default=None, help="report interval, ms")
    r.add_argument("cmd", nargs=argparse.REMAINDER)
    r.set_defaults(fn=cmd_run)

    e = sub.add_parser("env", help="print tracer environment variables")
    e.add_argument("--socket", default=default_socket())
    e.add_argument("--interval", type=int, default=None)
    e.add_argument("--systemd", action="store_true", help="print as systemd drop-in")
    e.set_defaults(fn=cmd_env)

    p = sub.add_parser("probe", help="one-shot RTSP stream check")
    p.add_argument("url", nargs="+")
    p.add_argument("-d", "--duration", type=float, default=10.0, help="seconds (default 10)")
    p.add_argument("-t", "--transport", choices=["tcp", "udp", "auto"], default="tcp")
    p.add_argument("--latency", type=int, default=200, help="rtspsrc latency, ms")
    p.add_argument("--json", action="store_true")
    p.add_argument("-v", "--verbose", action="store_true")
    p.set_defaults(fn=cmd_probe)

    st = sub.add_parser("status", help="show daemon state")
    st.add_argument("--host", default="127.0.0.1")
    st.add_argument("--port", type=int, default=DEFAULT_PORT)
    st.add_argument("--token", default=os.environ.get("GSTSCOPE_TOKEN", ""))
    st.add_argument("--json", action="store_true")
    st.set_defaults(fn=cmd_status)
    return ap


def main(argv: Optional[List[str]] = None) -> int:
    ap = build_parser()
    a = ap.parse_args(argv)
    if not getattr(a, "fn", None):
        ap.print_help()
        return 0
    return a.fn(a)

"""Receives tracer datagrams and keeps per-pipeline state + short history."""
from __future__ import annotations

import json
import logging
import os
import socket
import threading
from collections import deque
from typing import Any, Deque, Dict, List, Optional

from .util import now

log = logging.getLogger("gstscope.collector")

HISTORY = 300          # samples kept per series (5 min at 1 s)
STALE_AFTER = 5.0      # seconds without stats -> "stale"
FORGET_AFTER = 120.0   # seconds without stats -> removed
MAX_EVENTS = 200

# heuristics for hints
BUSY_UTIL = 0.80       # element busy >80% of the time
QUEUE_FULL = 0.80      # queue >80% full


class Pipeline:
    def __init__(self, key: str, msg: Dict[str, Any]):
        self.key = key
        self.pid = msg.get("pid")
        self.proc = msg.get("proc", "")
        self.name = msg.get("pipeline", "")
        self.uid = msg.get("uid", "")
        self.gst = ""
        self.tracer = ""
        self.state = ""
        self.first_seen = now()
        self.last_seen = now()
        self.elements: List[Dict[str, Any]] = []
        self.links: List[Dict[str, Any]] = []
        self.pads: Dict[str, Dict[str, Any]] = {}      # "el:pad" -> latest
        self.elstats: Dict[str, Dict[str, Any]] = {}   # id -> latest
        self.hist: Dict[str, Deque] = {}               # series -> deque[(t, v)]
        self.events: Deque[Dict[str, Any]] = deque(maxlen=MAX_EVENTS)
        self.hints: List[Dict[str, Any]] = []
        self.errors = 0
        self.warnings = 0
        # wall clock <-> tracer monotonic (ns) mapping for events
        self._ts_offset: Optional[float] = None

    # ------------------------------------------------------------------
    def _push(self, series: str, t: float, v: Optional[float]) -> None:
        if v is None:
            return
        d = self.hist.get(series)
        if d is None:
            d = self.hist[series] = deque(maxlen=HISTORY)
        d.append((round(t, 2), round(v, 3)))

    def apply_topology(self, msg: Dict[str, Any]) -> None:
        self.gst = msg.get("gst", self.gst)
        self.tracer = msg.get("tracer", self.tracer)
        self.state = msg.get("state", self.state)
        self.elements = msg.get("elements", [])
        self.links = msg.get("links", [])
        self._order()

    def _order(self) -> None:
        """Assign a 'depth' (column in the graph) and sort elements upstream->downstream."""
        ids = [e["id"] for e in self.elements]
        incoming = {i: [] for i in ids}  # type: Dict[str, List[str]]
        for l in self.links:
            if l["sink"] in incoming and l["src"] in incoming:
                incoming[l["sink"]].append(l["src"])
        depth = {}  # type: Dict[str, int]

        def d(i: str, seen: frozenset) -> int:
            if i in depth:
                return depth[i]
            if i in seen:          # cycle guard
                return 0
            ups = incoming.get(i, [])
            v = 0 if not ups else 1 + max(d(u, seen | {i}) for u in ups)
            depth[i] = v
            return v

        for i in ids:
            d(i, frozenset())
        for e in self.elements:
            e["depth"] = depth.get(e["id"], 0)
        self.elements.sort(key=lambda e: (e["depth"], e["id"]))

    def apply_stats(self, msg: Dict[str, Any]) -> None:
        t = now()
        self.last_seen = t
        self.state = msg.get("state", self.state)
        if msg.get("ts") is not None:
            self._ts_offset = t - msg["ts"] / 1e9

        for p in msg.get("pads", []):
            k = "%s:%s" % (p["el"], p["pad"])
            self.pads[k] = p
            self._push("fps:" + k, t, p.get("fps"))
            self._push("kbps:" + k, t, p.get("kbps"))

        dt_s = max(msg.get("dt_ms", 1000.0), 1.0) / 1000.0
        for e in msg.get("elements", []):
            eid = e["id"]
            if "proc_n" in e and "queue" not in e:
                e["util"] = round(e["proc_avg_us"] * e["proc_n"] / (dt_s * 1e6), 3)
            q = e.get("queue")
            if q:
                fills = []
                if q.get("max_buffers"):
                    fills.append(q["buffers"] / float(q["max_buffers"]))
                if q.get("max_time_ms"):
                    fills.append(q["time_ms"] / float(q["max_time_ms"]))
                q["fill"] = round(max(fills), 3) if fills else 0.0
                self._push("queue:" + eid, t, q["fill"])
            self.elstats[eid] = e
            self._push("proc:" + eid, t, e.get("proc_avg_us"))
            self._push("procmax:" + eid, t, e.get("proc_max_us"))
            self._push("sink:" + eid, t, e.get("sink_avg_us"))

        self.apply_events(msg)
        self.hints = self._hints()

    def apply_events(self, msg: Dict[str, Any]) -> None:
        t = now()
        if self._ts_offset is None and msg.get("ts") is not None:
            self._ts_offset = t - msg["ts"] / 1e9
        for ev in msg.get("events", []):
            ev = dict(ev)
            if self._ts_offset is not None and "ts" in ev:
                ev["time"] = round(self._ts_offset + ev["ts"] / 1e9, 3)
            else:
                ev["time"] = round(t, 3)
            ev.pop("ts", None)
            if ev.get("level") == "error":
                self.errors += 1
            elif ev.get("level") == "warning":
                self.warnings += 1
            self.events.append(ev)

    # ------------------------------------------------------------------
    def _klass(self, eid: str) -> str:
        for e in self.elements:
            if e["id"] == eid:
                return e.get("klass", "")
        return ""

    def _is(self, eid: str, flag: str) -> bool:
        for e in self.elements:
            if e["id"] == eid:
                return bool(e.get(flag))
        return False

    def _hints(self) -> List[Dict[str, Any]]:
        hints = []
        queues = {}
        for eid, e in self.elstats.items():
            q = e.get("queue")
            if q is not None:
                queues[eid] = q
                if q.get("fill", 0) >= QUEUE_FULL:
                    down = [l["sink"] for l in self.links if l["src"] == eid]
                    hints.append({
                        "level": "warn", "element": eid,
                        "text": "%s is %d%% full: downstream (%s) cannot keep up"
                                % (eid, q["fill"] * 100, ", ".join(down) or "?"),
                    })
                continue
            util = e.get("util")
            if util is not None and util >= BUSY_UTIL and not self._is(eid, "sink"):
                hints.append({
                    "level": "warn" if util < 1.0 else "crit", "element": eid,
                    "text": "%s is busy %d%% of the time (avg %.1f ms/buffer): likely bottleneck"
                            % (eid, util * 100, e["proc_avg_us"] / 1000.0),
                })
            if e.get("qos_dropped"):
                hints.append({
                    "level": "warn", "element": eid,
                    "text": "%s dropped %d buffers (QoS, late)" % (eid, e["qos_dropped"]),
                })
        for k, p in self.pads.items():
            if p.get("flow_errors"):
                hints.append({
                    "level": "crit", "element": p["el"],
                    "text": "%s returned %s %d times" % (k, p.get("last_flow", "error"),
                                                         p["flow_errors"]),
                })
        idle = bool(self.pads) and all((p.get("fps") or 0) == 0 for p in self.pads.values())
        if idle and self.state == "PLAYING":
            hints.append({"level": "crit", "element": None,
                          "text": "pipeline is PLAYING but no buffers are flowing"})
        elif self.state in ("PAUSED", "READY") and now() - self.first_seen > 5 and (
                idle or not self.pads):
            full = [eid for eid, q in queues.items() if q.get("fill", 0) > 0]
            hints.append({"level": "crit", "element": full[0] if full else None,
                          "text": "pipeline stuck in %s with no data flow: usually a sink waiting "
                                  "for preroll; a full queue on another branch (%s) or an "
                                  "encoder with lookahead can deadlock a tee"
                                  % (self.state, ", ".join(full) or "none")})
        hints.sort(key=lambda h: 0 if h["level"] == "crit" else 1)
        return hints

    # ------------------------------------------------------------------
    def status(self) -> str:
        age = now() - self.last_seen
        return "live" if age < STALE_AFTER else "stale"

    def summary(self) -> Dict[str, Any]:
        sources = [p for k, p in self.pads.items() if self._is(p["el"], "source")]
        fps = max([p.get("fps", 0) for p in sources] or [0])
        return {
            "id": self.key, "pid": self.pid, "proc": self.proc, "name": self.name,
            "state": self.state, "status": self.status(), "gst": self.gst,
            "elements": len(self.elements), "source_fps": round(fps, 2),
            "errors": self.errors, "warnings": self.warnings,
            "hint_count": len(self.hints), "last_seen": round(self.last_seen, 3),
        }

    def detail(self, history: bool = False) -> Dict[str, Any]:
        d = self.summary()
        d.update({
            "tracer": self.tracer,
            "topology": {"elements": self.elements, "links": self.links},
            "pads": self.pads, "elstats": self.elstats,
            "hints": self.hints, "events": list(self.events)[-50:],
            "first_seen": round(self.first_seen, 3),
        })
        if history:
            d["history"] = {k: list(v) for k, v in self.hist.items()}
        return d


class Collector:
    def __init__(self, socket_path: str):
        self.socket_path = socket_path
        self.lock = threading.Lock()
        self.pipelines: Dict[str, Pipeline] = {}
        self.received = 0
        self.bad = 0
        self._sock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    def start(self) -> None:
        d = os.path.dirname(self.socket_path)
        if d and not os.path.isdir(d):
            os.makedirs(d, mode=0o755, exist_ok=True)
        try:
            os.unlink(self.socket_path)
        except FileNotFoundError:
            pass
        s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 << 20)
        s.bind(self.socket_path)
        os.chmod(self.socket_path, 0o666)   # any user's pipeline may report
        s.settimeout(1.0)
        self._sock = s
        self._thread = threading.Thread(target=self._run, name="collector", daemon=True)
        self._thread.start()
        log.info("listening for tracers on %s", self.socket_path)

    def stop(self) -> None:
        self._stop.set()
        if self._sock:
            self._sock.close()
        try:
            os.unlink(self.socket_path)
        except OSError:
            pass

    def _run(self) -> None:
        assert self._sock is not None
        last_gc = now()
        while not self._stop.is_set():
            try:
                data = self._sock.recv(1 << 21)
            except socket.timeout:
                data = None
            except OSError:
                break
            if data:
                self.feed(data)
            if now() - last_gc > 5:
                self._gc()
                last_gc = now()

    def feed(self, data: bytes) -> None:
        try:
            msg = json.loads(data.decode("utf-8", "replace"))
        except ValueError:
            self.bad += 1
            return
        if msg.get("v") != 1:
            self.bad += 1
            return
        key = "%s-%s" % (msg.get("pid"), msg.get("uid", "").replace("0x", ""))
        with self.lock:
            self.received += 1
            p = self.pipelines.get(key)
            if p is None:
                p = self.pipelines[key] = Pipeline(key, msg)
                log.info("new pipeline %s (%s pid %s)", p.name, p.proc, p.pid)
            t = msg.get("type")
            if t == "topology":
                p.apply_topology(msg)
            elif t == "stats":
                p.apply_stats(msg)
            elif t == "event":
                p.apply_events(msg)
                p.last_seen = now()

    def _gc(self) -> None:
        t = now()
        with self.lock:
            for k in [k for k, p in self.pipelines.items() if t - p.last_seen > FORGET_AFTER]:
                log.info("forgetting pipeline %s", k)
                del self.pipelines[k]

    # ------------------------------------------------------------------
    def list(self) -> List[Dict[str, Any]]:
        with self.lock:
            return [p.summary() for p in self.pipelines.values()]

    def snapshot(self) -> List[Dict[str, Any]]:
        with self.lock:
            return [p.detail(history=False) for p in self.pipelines.values()]

    def get(self, key: str, history: bool = True) -> Optional[Dict[str, Any]]:
        with self.lock:
            p = self.pipelines.get(key)
            return p.detail(history=history) if p else None

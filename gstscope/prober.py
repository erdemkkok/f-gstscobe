"""RTSP stream health probes built on GStreamer (rtspsrc ! parsebin ! fakesink)."""
from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from collections import deque
from typing import Any, Callable, Deque, Dict, List, Optional

import gi

gi.require_version("Gst", "1.0")
from gi.repository import GLib, Gst  # noqa: E402

from .util import mask_url, mono, now  # noqa: E402

log = logging.getLogger("gstscope.prober")

HISTORY = 300
STALL_AFTER = 3.0        # s without buffers -> stalled
RESTART_AFTER = 10.0     # s stalled / errored -> reconnect
MAX_BACKOFF = 60.0

_gst_ready = False


def gst_init() -> None:
    global _gst_ready
    if not _gst_ready:
        Gst.init(None)
        _gst_ready = True


class MainLoop:
    """One GLib main loop thread shared by all probes."""
    _inst: Optional["MainLoop"] = None

    def __init__(self) -> None:
        gst_init()
        self.loop = GLib.MainLoop()
        self.thread = threading.Thread(target=self.loop.run, name="glib", daemon=True)
        self.thread.start()

    @classmethod
    def get(cls) -> "MainLoop":
        if cls._inst is None:
            cls._inst = MainLoop()
        return cls._inst

    @staticmethod
    def call(fn: Callable, *args: Any) -> None:
        def _cb() -> bool:
            try:
                fn(*args)
            except Exception:  # pragma: no cover
                log.exception("main loop call failed")
            return False
        GLib.idle_add(_cb)


# ---------------------------------------------------------------------------

class StreamStats:
    """Per elementary stream (video/audio) measurements."""

    def __init__(self, pad_name: str):
        self.pad = pad_name
        self.media = ""
        self.codec = ""
        self.caps = ""
        self.width = None   # type: Optional[int]
        self.height = None  # type: Optional[int]
        self.nominal_fps = None  # type: Optional[float]
        self.profile = ""
        self.buffers = 0
        self.bytes = 0
        self.keyframes = 0
        self.first_buffer_mono = None  # type: Optional[float]
        self.last_arrival = None  # type: Optional[float]
        self.last_ts = None  # type: Optional[int]
        self.jitter = 0.0      # RFC 3550 style, seconds
        self._w_gap = 0.0      # largest inter-arrival gap in current tick
        self._gaps: Deque = deque(maxlen=10)   # per-tick maxima, last 10 s
        self.frames_since_key = 0
        self.last_key_arrival = None  # type: Optional[float]
        self.gop_frames = None  # type: Optional[int]
        self.gop_sec = None  # type: Optional[float]
        self.ts_backwards = 0
        # window counters
        self._w_buffers = 0
        self._w_bytes = 0
        self.fps = 0.0
        self.kbps = 0.0
        self.hist_fps: Deque = deque(maxlen=HISTORY)
        self.hist_kbps: Deque = deque(maxlen=HISTORY)
        self.hist_jitter: Deque = deque(maxlen=HISTORY)

    def set_caps(self, caps: Gst.Caps) -> None:
        if caps is None or caps.get_size() == 0:
            return
        s = caps.get_structure(0)
        name = s.get_name()
        self.caps = caps.to_string()[:300]
        self.media = name.split("/")[0]
        codec_map = {
            "video/x-h264": "H.264", "video/x-h265": "H.265", "image/jpeg": "MJPEG",
            "video/mpeg": "MPEG", "video/x-vp8": "VP8", "video/x-vp9": "VP9",
            "video/x-av1": "AV1", "audio/x-alaw": "G.711 A-law",
            "audio/x-mulaw": "G.711 u-law", "audio/mpeg": "AAC/MP3",
            "audio/x-opus": "Opus",
        }
        self.codec = codec_map.get(name, name)
        if name == "image/jpeg":
            self.media = "video"
        ok, w = s.get_int("width")
        if ok:
            self.width = w
        ok, h = s.get_int("height")
        if ok:
            self.height = h
        ok, num, den = s.get_fraction("framerate")
        if ok and den:
            self.nominal_fps = round(num / float(den), 2) if num else None
        prof = s.get_string("profile")
        if prof:
            self.profile = prof

    def on_buffer(self, buf: Gst.Buffer, t: float) -> None:
        self.buffers += 1
        self._w_buffers += 1
        size = buf.get_size()
        self.bytes += size
        self._w_bytes += size
        if self.first_buffer_mono is None:
            self.first_buffer_mono = t

        ts = buf.dts if buf.dts != Gst.CLOCK_TIME_NONE else buf.pts
        if self.last_arrival is not None:
            gap = t - self.last_arrival
            if gap > self._w_gap:
                self._w_gap = gap
            if ts != Gst.CLOCK_TIME_NONE and self.last_ts is not None:
                dts = (ts - self.last_ts) / 1e9
                if dts < 0:
                    self.ts_backwards += 1
                else:
                    d = abs(gap - dts)
                    self.jitter += (d - self.jitter) / 16.0
        self.last_arrival = t
        if ts != Gst.CLOCK_TIME_NONE:
            self.last_ts = ts

        if self.media == "video":
            if not buf.has_flags(Gst.BufferFlags.DELTA_UNIT):
                self.keyframes += 1
                if self.last_key_arrival is not None:
                    self.gop_frames = self.frames_since_key
                    self.gop_sec = round(t - self.last_key_arrival, 2)
                self.last_key_arrival = t
                self.frames_since_key = 1
            else:
                self.frames_since_key += 1

    def tick(self, dt: float, t_wall: float) -> None:
        self.fps = self._w_buffers / dt if dt > 0 else 0.0
        self.kbps = self._w_bytes * 8 / 1000.0 / dt if dt > 0 else 0.0
        self._w_buffers = 0
        self._w_bytes = 0
        self._gaps.append(self._w_gap)
        self._w_gap = 0.0
        tw = round(t_wall, 2)
        self.hist_fps.append((tw, round(self.fps, 2)))
        self.hist_kbps.append((tw, round(self.kbps, 1)))
        self.hist_jitter.append((tw, round(self.jitter * 1000, 2)))

    def to_dict(self, history: bool = False) -> Dict[str, Any]:
        d = {
            "pad": self.pad, "media": self.media, "codec": self.codec,
            "width": self.width, "height": self.height, "profile": self.profile,
            "nominal_fps": self.nominal_fps, "fps": round(self.fps, 2),
            "kbps": round(self.kbps, 1), "buffers": self.buffers, "bytes": self.bytes,
            "keyframes": self.keyframes, "gop_frames": self.gop_frames,
            "gop_sec": self.gop_sec, "jitter_ms": round(self.jitter * 1000, 2),
            "max_gap_ms": round(max(self._gaps or [0.0]) * 1000, 1),
            "ts_backwards": self.ts_backwards, "caps": self.caps,
            "active_s": round(mono() - self.first_buffer_mono, 2)
            if self.first_buffer_mono is not None else 0.0,
        }
        if history:
            d["history"] = {"fps": list(self.hist_fps), "kbps": list(self.hist_kbps),
                            "jitter_ms": list(self.hist_jitter)}
        return d


class Probe:
    def __init__(self, url: str, name: str = "", transport: str = "tcp",
                 latency: int = 200, pid: Optional[str] = None,
                 created: Optional[float] = None):
        self.id = pid or uuid.uuid4().hex[:8]
        self.url = url
        self.name = name or mask_url(url).split("@")[-1].split("/")[0]
        self.transport = transport if transport in ("tcp", "udp", "auto") else "tcp"
        self.latency = int(latency)
        self.created = created or now()

        self.lock = threading.Lock()
        self.pipeline = None  # type: Optional[Gst.Pipeline]
        self.state = "stopped"
        self.error = ""
        self.connects = 0
        self.reconnects = 0
        self.started_mono = None  # type: Optional[float]
        self.connect_ms = None  # type: Optional[float]
        self.streams: Dict[str, StreamStats] = {}
        self.server = ""
        self._last_tick = None  # type: Optional[float]
        self._bad_since = None  # type: Optional[float]
        self._backoff = RESTART_AFTER
        self._timer = 0
        self.running = False

    # -- config --------------------------------------------------------------
    def config(self) -> Dict[str, Any]:
        return {"id": self.id, "url": self.url, "name": self.name,
                "transport": self.transport, "latency": self.latency,
                "created": self.created}

    # -- lifecycle (main loop thread) -----------------------------------------
    def start(self) -> None:
        self.running = True
        self._build()
        if not self._timer:
            self._timer = GLib.timeout_add(1000, self._on_tick)

    def stop(self) -> None:
        self.running = False
        if self._timer:
            GLib.source_remove(self._timer)
            self._timer = 0
        self._teardown()
        with self.lock:
            self.state = "stopped"

    def _teardown(self) -> None:
        p = self.pipeline
        self.pipeline = None
        if p is not None:
            bus = p.get_bus()
            bus.remove_signal_watch()
            p.set_state(Gst.State.NULL)

    def _build(self) -> None:
        self._teardown()
        p = Gst.Pipeline.new("probe-" + self.id)
        src = Gst.ElementFactory.make("rtspsrc", None)
        if src is None:
            with self.lock:
                self.state = "error"
                self.error = "rtspsrc not available (install gstreamer1.0-plugins-good)"
            return
        src.set_property("location", self.url)
        src.set_property("latency", self.latency)
        if self.transport != "auto":
            Gst.util_set_object_arg(src, "protocols", self.transport)
        try:
            src.set_property("timeout", 5 * 1000 * 1000)   # us, udp timeout
            src.set_property("tcp-timeout", 10 * 1000 * 1000)
        except TypeError:
            pass
        src.connect("pad-added", self._on_src_pad)
        p.add(src)

        bus = p.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._on_message)

        with self.lock:
            self.streams = {}
            self.state = "connecting"
            self.error = ""
            self.started_mono = mono()
            self.connect_ms = None
            self.connects += 1
            self._last_tick = mono()
        self.pipeline = p
        if p.set_state(Gst.State.PLAYING) == Gst.StateChangeReturn.FAILURE:
            with self.lock:
                self.state = "error"
                self.error = self.error or "failed to start pipeline"

    def _on_src_pad(self, src: Gst.Element, pad: Gst.Pad) -> None:
        p = self.pipeline
        if p is None:
            return
        caps = pad.get_current_caps() or pad.query_caps(None)
        media = ""
        if caps and caps.get_size():
            media = caps.get_structure(0).get_string("media") or ""
        pb = Gst.ElementFactory.make("parsebin", None)
        if pb is None:   # very old GStreamer
            pb = Gst.ElementFactory.make("decodebin", None)
        pb.connect("pad-added", self._on_parsed_pad, pad.get_name(), media)
        p.add(pb)
        pb.sync_state_with_parent()
        pad.link(pb.get_static_pad("sink"))

    def _on_parsed_pad(self, pb: Gst.Element, pad: Gst.Pad, rtp_pad: str, media: str) -> None:
        p = self.pipeline
        if p is None:
            return
        sink = Gst.ElementFactory.make("fakesink", None)
        sink.set_property("sync", False)
        sink.set_property("async", False)
        p.add(sink)
        sink.sync_state_with_parent()
        pad.link(sink.get_static_pad("sink"))
        key = "%s/%s" % (rtp_pad, pad.get_name())
        st = StreamStats(key)
        if media:
            st.media = media
        caps = pad.get_current_caps()
        if caps:
            st.set_caps(caps)
        with self.lock:
            self.streams[key] = st
        pad.add_probe(Gst.PadProbeType.BUFFER, self._on_buffer, st)
        pad.connect("notify::caps", self._on_caps, st)

    def _on_caps(self, pad: Gst.Pad, _pspec: Any, st: StreamStats) -> None:
        caps = pad.get_current_caps()
        if caps:
            with self.lock:
                st.set_caps(caps)

    def _on_buffer(self, pad: Gst.Pad, info: Gst.PadProbeInfo, st: StreamStats):
        buf = info.get_buffer()
        if buf is not None:
            t = mono()
            with self.lock:
                if not st.codec:
                    caps = pad.get_current_caps()
                    if caps:
                        st.set_caps(caps)
                st.on_buffer(buf, t)
                if self.connect_ms is None and self.started_mono is not None:
                    self.connect_ms = round((t - self.started_mono) * 1000, 1)
        return Gst.PadProbeReturn.OK

    def _on_message(self, bus: Gst.Bus, msg: Gst.Message) -> None:
        t = msg.type
        if t == Gst.MessageType.ERROR:
            err, dbg = msg.parse_error()
            with self.lock:
                self.state = "error"
                self.error = err.message
            log.warning("probe %s error: %s", self.name, err.message)
        elif t == Gst.MessageType.EOS:
            with self.lock:
                self.state = "error"
                self.error = "end of stream (server closed the session)"
        elif t == Gst.MessageType.ELEMENT:
            s = msg.get_structure()
            if s and s.get_name() == "GstRTSPSrcTimeout":
                with self.lock:
                    self.state = "stalled"
                    self.error = "rtspsrc timeout (no data)"

    def _on_tick(self) -> bool:
        t = mono()
        tw = now()
        with self.lock:
            dt = t - (self._last_tick or t)
            self._last_tick = t
            for st in self.streams.values():
                st.tick(dt, tw)
            last = max([s.last_arrival for s in self.streams.values()
                        if s.last_arrival is not None] or [0])
            if self.state in ("connecting", "playing", "stalled") and last:
                if t - last > STALL_AFTER:
                    if self.state != "stalled":
                        self.state = "stalled"
                        self.error = "no data for %.0f s" % (t - last)
                else:
                    self.state = "playing"
                    self.error = ""
            elif self.state == "connecting" and self.started_mono and \
                    t - self.started_mono > RESTART_AFTER:
                self.state = "error"
                self.error = self.error or "no data after %.0f s" % RESTART_AFTER

            bad = self.state in ("error", "stalled")
            if not bad:
                self._bad_since = None
                self._backoff = RESTART_AFTER
            elif self._bad_since is None:
                self._bad_since = t
            restart = bad and self.running and t - self._bad_since > self._backoff
        if restart:
            log.info("probe %s: reconnecting", self.name)
            with self.lock:
                self.reconnects += 1
                self._bad_since = None
                self._backoff = min(self._backoff * 2, MAX_BACKOFF)
            self._build()
        return True

    # -- reporting -----------------------------------------------------------
    def to_dict(self, history: bool = False) -> Dict[str, Any]:
        with self.lock:
            streams = [s.to_dict(history) for s in self.streams.values()]
            video = next((s for s in streams if s["media"] == "video"), None)
            d = {
                "id": self.id, "name": self.name, "url": mask_url(self.url),
                "transport": self.transport, "latency": self.latency,
                "state": self.state, "error": self.error,
                "connects": self.connects, "reconnects": self.reconnects,
                "connect_ms": self.connect_ms,
                "uptime": round(mono() - self.started_mono, 1)
                if self.started_mono and self.state == "playing" else 0,
                "streams": streams, "video": video,
            }
        return d


class Prober:
    def __init__(self, store_path: Optional[str] = None):
        self.loop = MainLoop.get()
        self.store_path = store_path
        self.probes: Dict[str, Probe] = {}
        self.lock = threading.Lock()

    def load(self) -> None:
        if not self.store_path or not os.path.isfile(self.store_path):
            return
        try:
            with open(self.store_path) as f:
                items = json.load(f)
        except (OSError, ValueError) as e:
            log.warning("cannot read %s: %s", self.store_path, e)
            return
        for it in items:
            self.add(it["url"], it.get("name", ""), it.get("transport", "tcp"),
                     it.get("latency", 200), it.get("id"), it.get("created"), save=False)

    def save(self) -> None:
        if not self.store_path:
            return
        os.makedirs(os.path.dirname(self.store_path), exist_ok=True)
        tmp = self.store_path + ".tmp"
        with self.lock:
            items = [p.config() for p in self.probes.values()]
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w") as f:
            json.dump(items, f, indent=2)
        os.replace(tmp, self.store_path)

    def add(self, url: str, name: str = "", transport: str = "tcp", latency: int = 200,
            pid: Optional[str] = None, created: Optional[float] = None,
            save: bool = True) -> Probe:
        if not url.startswith(("rtsp://", "rtsps://", "rtspt://")):
            raise ValueError("url must start with rtsp://")
        p = Probe(url, name, transport, latency, pid, created)
        with self.lock:
            self.probes[p.id] = p
        MainLoop.call(p.start)
        if save:
            self.save()
        return p

    def remove(self, pid: str) -> bool:
        with self.lock:
            p = self.probes.pop(pid, None)
        if not p:
            return False
        MainLoop.call(p.stop)
        self.save()
        return True

    def restart(self, pid: str) -> bool:
        with self.lock:
            p = self.probes.get(pid)
        if not p:
            return False
        MainLoop.call(p.start)
        return True

    def list(self, history: bool = False) -> List[Dict[str, Any]]:
        with self.lock:
            probes = list(self.probes.values())
        return [p.to_dict(history) for p in probes]

    def get(self, pid: str, history: bool = True) -> Optional[Dict[str, Any]]:
        with self.lock:
            p = self.probes.get(pid)
        return p.to_dict(history) if p else None

    def stop_all(self) -> None:
        with self.lock:
            probes = list(self.probes.values())
        for p in probes:
            MainLoop.call(p.stop)

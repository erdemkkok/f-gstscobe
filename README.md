# gstscope

**GStreamer pipeline profiler + RTSP stream checker for Orange Pi (RK3588), NVIDIA Jetson and other Linux boards.**

gstscope answers two questions you ask every day when working with video on embedded boards:

1. **"Where is my pipeline slow?"** – a tracer plugin measures every element in a *running* pipeline
   (processing time, how busy it is, fps / bitrate on every link, queue fill, QoS drops, flow errors)
   and the web UI draws the pipeline graph with the bottleneck highlighted.
2. **"Is it the camera, the network or our software?"** – RTSP probes stay connected to your cameras / DVRs
   and track codec, resolution, real fps vs nominal, bitrate, GOP, jitter, gaps and reconnects.

Everything is served from the board itself: open `http://<board-ip>:8090`.

![pipeline view](docs/pipelines.png)
![RTSP probes](docs/probes.png)

```
┌──────────────────────┐ unix   ┌────────────────────────────┐
│ your app / pipeline  │ dgram  │ gstscope serve (daemon)    │── HTTP + SSE ──▶ web UI
│ + libgstscope.so     │──────▶ │  collector · RTSP prober   │── CLI: gstscope status
│   (GST_TRACERS)      │        └────────────────────────────┘
└──────────────────────┘
```

- The tracer never blocks your application: it sends one JSON datagram per pipeline per second with
  `MSG_DONTWAIT`. If the daemon is not running the data is simply dropped.
- No changes to your code: enable it with one environment variable.
- Daemon is Python 3.8+ standard library + PyGObject (already on Jetson / Orange Pi images). No Node, no pip.

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/erdemkkok/gstscope/main/scripts/install.sh | sudo sh
```

or download the `.deb` for your architecture (`arm64` / `amd64`) from Releases and `sudo apt install ./gstscope_*.deb`.

Supported: Ubuntu / Debian based images with GStreamer ≥ 1.16 – Jetson JetPack 5/6 (L4T), Orange Pi Ubuntu 22.04 (Rockchip MPP / RGA elements),
Raspberry Pi OS 64-bit, desktop Ubuntu.

The package installs and starts the `gstscope` systemd service.

## Quick start

```sh
# trace a gst-launch pipeline
gstscope run "v4l2src device=/dev/video0 ! video/x-raw,width=1920,height=1080 ! queue ! mpph264enc ! h264parse ! fakesink"

# trace any program (Python, C++, Qt, DeepStream app ...)
gstscope run -- ./my_app --config cfg.yaml

# check cameras from the terminal (no daemon needed)
gstscope probe rtsp://admin:pass@192.168.1.64:554/Streaming/Channels/101 -d 10
gstscope probe rtsp://cam1/... rtsp://cam2/... --json

# what the daemon currently sees
gstscope status
```

### Tracing a systemd service

```sh
gstscope env --systemd
sudo systemctl edit my-video.service      # paste the printed [Service] Environment= line
sudo systemctl restart my-video.service
```

which is simply:

```ini
[Service]
Environment="GST_TRACERS=gstscope"
```

Tracer options: `GST_TRACERS="gstscope(interval=500,socket=/tmp/gstscope.sock)"`
or the environment variables `GSTSCOPE_INTERVAL` (ms, ≥ 100) and `GSTSCOPE_SOCKET`.
Other tracers can be combined: `GST_TRACERS="gstscope;latency"`.

## What is measured

| metric | how | notes |
|---|---|---|
| fps / bitrate per link | buffers and bytes pushed on every src pad | ghost pads of bins (decodebin, uridecodebin, nvurisrcbin…) are resolved to the real elements |
| processing time per element | time from a buffer entering the element to the next buffer leaving it (FIFO matched) | exact for 1-in/1-out elements; approximate for muxers, demuxers, elements that change the buffer count. For `queue` this is the time a buffer waited in the queue |
| busy % | `avg processing time × buffers/s` | ≥ 80 % → bottleneck hint. Asynchronous HW decoders/encoders (MPP, NVENC/NVDEC) can show > 100 % because frames overlap in the pipeline – read it as latency |
| sink push time | duration of the push into a sink | includes waiting for the clock when `sync=true` |
| queue fill | `current-level-buffers / max-size-buffers` and time level | `queue` and `queue2` |
| QoS drops | from QOS messages | sinks / elements that drop late buffers |
| errors / warnings / EOS / state | bus messages | shown in the events panel |

Hints are generated for: busy elements, full queues (and which element downstream cannot keep up),
QoS drops, non-OK flow returns, "PLAYING but nothing flows", and pipelines stuck in PAUSED (preroll deadlocks,
typically a tee with a too-small queue on one branch).

Limitations (v0.1): pull-mode links (e.g. `filesrc ! qtdemux`) are not counted; `multiqueue` fill is not reported.

### RTSP probes

Each probe runs `rtspsrc ! parsebin ! fakesink` (no decoding, so it is cheap – dozens of cameras are fine)
and reports codec, resolution, profile, fps vs nominal fps, bitrate, GOP (frames / seconds), timestamp jitter,
maximum inter-frame gap, time to first frame and reconnects. Stalled or broken streams are reconnected
automatically with back-off. Probes added in the web UI are saved in `/var/lib/gstscope/probes.json`;
passwords are never shown in the UI or API.

## Configuration

`/etc/default/gstscope`:

```sh
GSTSCOPE_BIND=0.0.0.0
GSTSCOPE_PORT=8090
GSTSCOPE_TOKEN=            # set it to require http://<board>:8090/?token=...
GSTSCOPE_SOCKET=/run/gstscope/gstscope.sock
```

The UI has no authentication by default. Set `GSTSCOPE_TOKEN` or `GSTSCOPE_BIND=127.0.0.1` on boards that are
reachable from untrusted networks.

## HTTP API

| | |
|---|---|
| `GET /api/v1/status` | daemon info |
| `GET /api/v1/snapshot` | everything (pipelines with latest stats, probes) |
| `GET /api/v1/stream` | Server-Sent Events, one `snapshot` per second |
| `GET /api/v1/pipelines` · `/pipelines/<id>` | list · detail with 5 min history |
| `GET /api/v1/probes` · `/probes/<id>` | list · detail with history |
| `POST /api/v1/probes` | `{"url": "rtsp://...", "name": "...", "transport": "tcp"}` |
| `POST /api/v1/probes/<id>/restart` · `DELETE /api/v1/probes/<id>` | |

With a token: `Authorization: Bearer <token>` or `?token=<token>`.

## Build from source

```sh
sudo apt install build-essential meson ninja-build pkg-config libgstreamer1.0-dev python3-gi gir1.2-gstreamer-1.0
meson setup build && ninja -C build

# run without installing
python3 -m gstscope serve --socket /tmp/gstscope.sock &
GST_PLUGIN_PATH=$PWD/build python3 -m gstscope run --socket /tmp/gstscope.sock "videotestsrc ! queue ! fakesink"

# local RTSP test server for the prober
python3 tests/rtsp_server.py      # rtsp://127.0.0.1:8554/test and /stall

./scripts/build-deb.sh            # -> dist/gstscope_<ver>_<arch>.deb
```

## Roadmap

- per-element CPU (thread) usage, and RK3588 VPU / RGA / NPU load next to the elements (shared with [rkmon](https://github.com/erdemkkok/rkmon))
- Jetson NVENC / NVDEC utilisation
- decode test mode for probes (`mppvideodec` / `nvv4l2decoder`) and ONVIF / ISAPI discovery
- Prometheus `/metrics`, multi-board view
- terminal UI (`gstscope top`)

## License

MIT

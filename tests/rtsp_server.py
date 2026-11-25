#!/usr/bin/env python3
"""Local RTSP test server: rtsp://127.0.0.1:8554/test (H.264 + AAC-less PCMA audio)
and rtsp://127.0.0.1:8554/stall (stops sending after 5 s)."""
import sys
import gi
gi.require_version("Gst", "1.0")
gi.require_version("GstRtspServer", "1.0")
from gi.repository import GLib, Gst, GstRtspServer

Gst.init(None)
port = sys.argv[1] if len(sys.argv) > 1 else "8554"
srv = GstRtspServer.RTSPServer.new()
srv.set_service(port)
m = srv.get_mount_points()

def add(path, launch):
    f = GstRtspServer.RTSPMediaFactory.new()
    f.set_launch(launch)
    f.set_shared(True)
    m.add_factory(path, f)

add("/test", "( videotestsrc is-live=true ! video/x-raw,width=1280,height=720,framerate=25/1 "
             "! x264enc tune=zerolatency speed-preset=ultrafast key-int-max=50 bitrate=2000 "
             "! rtph264pay name=pay0 pt=96 "
             "audiotestsrc is-live=true ! alawenc ! rtppcmapay name=pay1 pt=8 )")
add("/stall", "( videotestsrc is-live=true num-buffers=125 ! video/x-raw,width=640,height=360,framerate=25/1 "
              "! x264enc tune=zerolatency speed-preset=ultrafast key-int-max=25 "
              "! rtph264pay name=pay0 pt=96 )")
srv.attach(None)
print("rtsp test server on", port, flush=True)
GLib.MainLoop().run()

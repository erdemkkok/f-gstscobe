#!/bin/sh
PYTHONPATH="/usr/lib/gstscope${PYTHONPATH:+:$PYTHONPATH}" exec /usr/bin/python3 -m gstscope "$@"

#!/bin/sh
set -eu

export DISPLAY="${DISPLAY:-:99}"

Xvfb "$DISPLAY" -screen 0 1365x900x24 -ac -nolisten tcp &
sleep 1
x11vnc -display "$DISPLAY" -forever -shared -nopw -localhost -rfbport 5900 >/tmp/x11vnc.log 2>&1 &
websockify --web=/usr/share/novnc/ 0.0.0.0:6080 localhost:5900 &

exec python -m fb_monitor.browser_login

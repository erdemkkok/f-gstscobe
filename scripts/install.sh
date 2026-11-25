#!/bin/sh
# gstscope installer:  curl -fsSL https://raw.githubusercontent.com/erdemkkok/f-gstscobe/main/scripts/install.sh | sudo sh
set -eu
REPO="erdemkkok/f-gstscobe"

[ "$(id -u)" -eq 0 ] || { echo "please run as root (sudo)"; exit 1; }
command -v apt-get >/dev/null || { echo "apt-based distro required"; exit 1; }

ARCH=$(dpkg --print-architecture)
case "$ARCH" in arm64|amd64) ;; *) echo "unsupported architecture: $ARCH"; exit 1;; esac

echo ">> looking up latest gstscope release"
URL=$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" \
      | grep -o "https://[^\"]*gstscope_[^\"]*_${ARCH}\.deb" | head -n1)
[ -n "$URL" ] || { echo "no .deb for $ARCH found in the latest release"; exit 1; }

TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
echo ">> downloading $URL"
curl -fsSL -o "$TMP/gstscope.deb" "$URL"
chmod 644 "$TMP/gstscope.deb"

echo ">> installing"
apt-get update -qq || true
apt-get install -y "$TMP/gstscope.deb"

IP=$(hostname -I 2>/dev/null | awk '{print $1}')
echo
echo "gstscope installed."
echo "  web UI:        http://${IP:-<board-ip>}:8090"
echo "  trace a pipe:  gstscope run \"videotestsrc ! fakesink\""
echo "  check camera:  gstscope probe rtsp://user:pass@<ip>:554/..."

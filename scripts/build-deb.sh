#!/bin/sh
# Build gstscope_<version>_<arch>.deb for the current machine.
# Needs: build-essential meson ninja-build pkg-config libgstreamer1.0-dev
# Build on the oldest target distro (e.g. Ubuntu 20.04) so the .deb runs on newer ones.
set -eu

ROOT=$(cd "$(dirname "$0")/.." && pwd)
VERSION=$(cat "$ROOT/VERSION")
ARCH=$(dpkg --print-architecture)
PLUGINDIR=$(pkg-config --variable=pluginsdir gstreamer-1.0)
OUT=${OUT:-$ROOT/dist}
STAGE=$(mktemp -d)
trap 'rm -rf "$STAGE" "$ROOT/build-deb"' EXIT

echo ">> gstscope $VERSION ($ARCH), plugin dir $PLUGINDIR"

# 1) tracer plugin
rm -rf "$ROOT/build-deb"
meson setup "$ROOT/build-deb" "$ROOT" --prefix=/usr -Dplugindir="$PLUGINDIR" >/dev/null
ninja -C "$ROOT/build-deb"
DESTDIR="$STAGE" meson install -C "$ROOT/build-deb" --no-rebuild >/dev/null 2>&1 || \
    DESTDIR="$STAGE" ninja -C "$ROOT/build-deb" install >/dev/null
strip --strip-unneeded "$STAGE$PLUGINDIR/libgstscope.so"

# 2) python daemon + web UI
install -d "$STAGE/usr/lib/gstscope/gstscope" "$STAGE/usr/share/gstscope/web" \
           "$STAGE/usr/bin" "$STAGE/lib/systemd/system" "$STAGE/etc/default" \
           "$STAGE/usr/share/doc/gstscope"
install -m 644 "$ROOT"/gstscope/*.py "$STAGE/usr/lib/gstscope/gstscope/"
sed -i "s/^__version__ = .*/__version__ = \"$VERSION\"/" "$STAGE/usr/lib/gstscope/gstscope/__init__.py"
install -m 644 "$ROOT"/web/* "$STAGE/usr/share/gstscope/web/"
install -m 755 "$ROOT/packaging/gstscope.sh" "$STAGE/usr/bin/gstscope"
install -m 644 "$ROOT/packaging/gstscope.service" "$STAGE/lib/systemd/system/"
install -m 644 "$ROOT/packaging/default" "$STAGE/etc/default/gstscope"
install -m 644 "$ROOT/README.md" "$ROOT/LICENSE" "$STAGE/usr/share/doc/gstscope/" 2>/dev/null || true

# 3) control
install -d "$STAGE/DEBIAN"
SIZE=$(du -sk "$STAGE" | cut -f1)
sed -e "s/@VERSION@/$VERSION/" -e "s/@ARCH@/$ARCH/" -e "s/@SIZE@/$SIZE/" \
    "$ROOT/packaging/debian/control.in" > "$STAGE/DEBIAN/control"
install -m 755 "$ROOT/packaging/debian/postinst" "$ROOT/packaging/debian/prerm" \
               "$ROOT/packaging/debian/postrm" "$STAGE/DEBIAN/"
echo "/etc/default/gstscope" > "$STAGE/DEBIAN/conffiles"

mkdir -p "$OUT"
DEB="$OUT/gstscope_${VERSION}_${ARCH}.deb"
dpkg-deb --root-owner-group --build "$STAGE" "$DEB" >/dev/null 2>&1 || \
    fakeroot dpkg-deb --build "$STAGE" "$DEB" >/dev/null
echo ">> $DEB"

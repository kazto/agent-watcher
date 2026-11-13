#!/bin/sh
# Build a redistributable tarball: binary + README + LICENSE + systemd unit.
set -eu
cd "$(dirname "$0")/.."
cargo build --release
VERSION=$(grep -m1 '^version' Cargo.toml | sed 's/.*"\(.*\)".*/\1/')
TARGET=$(rustc -vV | sed -n 's/^host: //p')
DIST="dist/agent-watcher-${VERSION}-${TARGET}"
rm -rf "$DIST"
mkdir -p "$DIST"
cp target/release/agent-watcher "$DIST/"
cp README.md LICENSE "$DIST/"
cp systemd/agent-watcher.service "$DIST/"
cp watch.json.example "$DIST/" 2>/dev/null || true
tar -czf "dist/agent-watcher-${VERSION}-${TARGET}.tar.gz" -C dist "agent-watcher-${VERSION}-${TARGET}"
echo "dist/agent-watcher-${VERSION}-${TARGET}.tar.gz"

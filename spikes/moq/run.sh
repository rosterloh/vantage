#!/usr/bin/env bash
#
# MoQ transport spike: builds (if needed) the moq-relay binary and the moq-gst
# GStreamer plugin, starts a local relay with an auto-generated self-signed cert,
# and runs the in-process latency probe (videotestsrc -> x264enc -> moqsink ->
# relay -> moqsrc -> avdec_h264). Prints publish->relay->subscribe latency.
#
# Usage:
#   MOQ_DIR=/path/to/checkout/of/github.com/moq-dev/moq ./run.sh
#   ./run.sh        # clones moq into ./.moq if MOQ_DIR is unset
#
# Requires: rust, git, system GStreamer 1.22+ with x264enc/avdec_h264 (the same
# packages vantage-robot/-client already need).
set -euo pipefail
cd "$(dirname "$0")"

MOQ_DIR="${MOQ_DIR:-$PWD/.moq}"
SECS="${SECS:-12}"

if [[ ! -d "$MOQ_DIR" ]]; then
  echo ">> cloning moq into $MOQ_DIR"
  git clone --filter=blob:none --depth 1 https://github.com/moq-dev/moq.git "$MOQ_DIR"
fi

echo ">> building moq-relay + moq-gst (release)"
( cd "$MOQ_DIR" && cargo build --release -p moq-relay -p moq-gst )

PLUGIN_DIR="$MOQ_DIR/target/release"
RELAY="$PLUGIN_DIR/moq-relay"
test -f "$PLUGIN_DIR/libgstmoq.so" || test -f "$PLUGIN_DIR/libgstmoq.dylib"

echo ">> starting relay on 127.0.0.1:4443 (self-signed, auth disabled)"
RUST_LOG=info "$RELAY" localhost.toml &
RELAY_PID=$!
trap 'kill $RELAY_PID 2>/dev/null || true' EXIT
sleep 2

echo ">> building + running latency probe (${SECS}s)"
export GST_PLUGIN_PATH_1_0="$PLUGIN_DIR"
export MOQ_URL="https://localhost:4443"
export SECS
cargo run --release 2>/dev/null

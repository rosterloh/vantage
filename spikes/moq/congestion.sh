#!/usr/bin/env bash
#
# Congestion / adaptive-bitrate test. Drives a high-entropy (incompressible)
# encode well above an emulated bandwidth cap, and compares how each path copes:
#   - MoQ (fixed bitrate, no media congestion control)
#   - WebRTC fixed bitrate (ADAPT=0)
#   - WebRTC with rtpgccbwe driving x264enc bitrate (ADAPT=1) — vantage's wiring.
#
# rtpgccbwe lives in gst-plugins-rs (rsrtp); it is NOT installed by default (so
# vantage's own adaptive bitrate is currently host-deferred too). This builds it
# into ./.gst-plugins-rs on first run.
set -uo pipefail
cd "$(dirname "$0")"
MOQ_DIR="${MOQ_DIR:-$PWD/.moq}"
BUILD="$MOQ_DIR/target/release"
GPRS_DIR="${GPRS_DIR:-$PWD/.gst-plugins-rs}"
RSRTP="$GPRS_DIR/target/release"
SECS="${SECS:-30}"
BITRATE="${BITRATE:-4000}"            # encoder target (kbit/s)
CAP="${CAP:-rate 2mbit delay 10ms}"   # bandwidth cap (~2 Mbit/s; GCC can converge)
export PATTERN=snow                    # incompressible noise -> x264 actually emits ~BITRATE

command -v docker >/dev/null || { echo "docker required (unprivileged netem on lo)"; exit 1; }
test -x "$BUILD/moq-relay" || { echo "run ./run.sh first to build moq-relay"; exit 1; }
if [[ ! -f "$RSRTP/libgstrsrtp.so" ]]; then
  echo ">> building rtpgccbwe (gst-plugin-rtp) into $GPRS_DIR (one-time)"
  [[ -d "$GPRS_DIR" ]] || git clone --filter=blob:none --depth 1 \
    https://gitlab.freedesktop.org/gstreamer/gst-plugins-rs.git "$GPRS_DIR"
  ( cd "$GPRS_DIR" && cargo build --release -p gst-plugin-rtp )
fi
cargo build --release -q

docker rm -f netem >/dev/null 2>&1 || true
docker run -d --rm --name netem --cap-add NET_ADMIN --network host alpine sleep 900 >/dev/null
docker exec netem apk add -q iproute2 >/dev/null 2>&1
docker exec netem tc qdisc replace dev lo root netem $CAP 2>/dev/null

RUST_LOG=warn "$BUILD/moq-relay" localhost.toml >/dev/null 2>&1 &
RELAY=$!
trap 'docker exec netem tc qdisc del dev lo root 2>/dev/null||true; docker rm -f netem >/dev/null 2>&1||true; kill "$RELAY" 2>/dev/null||true' EXIT
sleep 2

echo "### encoder target ${BITRATE} kbit/s into [$CAP] for ${SECS}s"; echo
echo "-- MoQ (fixed ${BITRATE}, no congestion control) --"
GST_PLUGIN_PATH_1_0="$BUILD" MOQ_URL=https://localhost:4443 SECS=$SECS BITRATE=$BITRATE \
  ./target/release/moq-lat 2>/dev/null | grep -E 'matched|latency'
echo
echo "-- WebRTC ADAPT=0 (fixed ${BITRATE}) --"
SECS=$SECS BITRATE=$BITRATE ADAPT=0 ./target/release/webrtc-lat 2>/dev/null | grep -E 'matched|tx_frames|latency'
echo
echo "-- WebRTC ADAPT=1 (rtpgccbwe -> x264enc) --"
GST_PLUGIN_PATH_1_0="$RSRTP" SECS=$SECS BITRATE=$BITRATE ADAPT=1 \
  ./target/release/webrtc-lat 2>&1 | grep -E 'matched|tx_frames|latency|rtpgccbwe'

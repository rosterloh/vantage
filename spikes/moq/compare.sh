#!/usr/bin/env bash
#
# MoQ vs webrtcbin transport latency under emulated network conditions.
# Runs both probes (post-encode H.264 AU -> transport -> pre-decode AU, matched
# per-frame) across a netem matrix and prints a table.
#
# Network shaping is applied to the host loopback via a NET_ADMIN container
# (docker group, no sudo). MoQ media traverses the relay (2 hops over lo);
# WebRTC is direct P2P (1 hop) — that asymmetry is the point.
#
# Usage:  MOQ_DIR=/path/to/moq ./compare.sh      (defaults to ./.moq; run ./run.sh once first to populate it)
set -uo pipefail
cd "$(dirname "$0")"
MOQ_DIR="${MOQ_DIR:-$PWD/.moq}"
BUILD="$MOQ_DIR/target/release"
SECS="${SECS:-10}"

command -v docker >/dev/null || { echo "docker required (for unprivileged netem on lo)"; exit 1; }
test -x "$BUILD/moq-relay" || { echo "build moq first: MOQ_DIR=$MOQ_DIR ./run.sh"; exit 1; }
cargo build --release -q

docker rm -f netem >/dev/null 2>&1 || true
docker run -d --rm --name netem --cap-add NET_ADMIN --network host alpine sleep 600 >/dev/null
docker exec netem apk add -q iproute2 >/dev/null 2>&1
tcset() { docker exec netem tc qdisc replace dev lo root netem $1 2>/dev/null; }
tcdel() { docker exec netem tc qdisc del dev lo root 2>/dev/null || true; }

RUST_LOG=warn "$BUILD/moq-relay" localhost.toml >/dev/null 2>&1 &
RELAY=$!
trap 'tcdel; docker rm -f netem >/dev/null 2>&1 || true; kill "$RELAY" 2>/dev/null || true' EXIT
sleep 2

run_moq()    { GST_PLUGIN_PATH_1_0="$BUILD" MOQ_URL=https://localhost:4443 SECS=$SECS ./target/release/moq-lat  2>/dev/null | grep -E 'matched|latency'; }
run_webrtc() { SECS=$SECS ./target/release/webrtc-lat 2>/dev/null | grep -E 'matched|tx_frames|latency'; }

for cond in "clean|" "delay25ms+loss1%|delay 25ms loss 1%" "loss5%|loss 5%" "delay50ms|delay 50ms"; do
  name=${cond%%|*}; prof=${cond#*|}
  [[ -z "$prof" ]] && tcdel || tcset "$prof"
  echo "######## $name  [netem: ${prof:-none}]"
  echo "-- MoQ (publish->relay->subscribe, 2 hops) --"; run_moq
  echo "-- WebRTC (sender->receiver, P2P, 1 hop)   --"; run_webrtc
  echo
done

# Spike: Media over QUIC (MoQ) as a transport vs WebRTC

**Branch:** `spike/moq-transport` · **Status:** throwaway spike, not for merge.

Question being de-risked: *can we carry Vantage's exact H.264 stream over MoQ
instead of `webrtcbin`, and what does the transport cost?*

## What this proves

Vantage's robot already produces `video/x-h264, stream-format=byte-stream,
alignment=au` out of its hardware/x264 encoder. The `moq-gst` plugin exposes two
GStreamer elements — `moqsink` / `moqsrc` — that accept exactly those caps. So the
swap is, at the pipeline level, dropping `webrtcbin` for:

```
... ! x264enc ! h264parse config-interval=-1 ! moqsink url=… broadcast=…   (robot)
moqsrc url=… broadcast=… ! h264parse ! avdec_h264 ! …                       (client)
```

No RTP payloader, no SDP, no ICE. The relay is always in the path (publisher and
subscriber both dial out to it). `tls-disable-verify=true` lets the client trust
the relay's self-signed dev cert with no fingerprint dance.

## Result (localhost loopback, single subscriber)

`src/main.rs` runs publisher + subscriber in one process and timestamps each
H.264 access unit at the `moqsink` boundary and again as it leaves `moqsrc`,
matching frames by PTS. Measured between post-encode and pre-decode, so it is the
**MoQ transport contribution only** (encode/decode excluded — those are identical
for WebRTC and MoQ).

```
matched_frames=360 unmatched_sent=0
MoQ transport latency (ms): min=0.4 p50=0.8 mean=0.8 p90=1.0 p99=1.4 max=20.9
```

- **Zero loss** (360/360 over 12 s) — QUIC is reliable+ordered.
- **Sub-millisecond median** transport overhead on loopback.
- The 20.9 ms `max` is the one-off connection/first-keyframe setup.

## What this does NOT prove (and why it matters)

This is the honest part — loopback flatters MoQ and is **not** where the
WebRTC-vs-MoQ tradeoff lives:

1. **No real network.** On localhost both MoQ and WebRTC show sub-ms transport
   latency. The real differentiators only appear with RTT + loss:
   - MoQ keeps a **relay in the path** → adds the operator↔relay↔robot RTT. On a
     LAN, `webrtcbin`'s ICE gives a *direct* host-to-host hop with no relay.
   - Under loss, WebRTC plays through with concealment (unreliable RTP + NACK/PLI);
     QUIC retransmits in-order (head-of-line risk). Not exercised here.
2. **No congestion control / adaptive bitrate.** Vantage's robot already runs
   transport-cc + `rtpgccbwe` (300–2500 kbit/s) via `webrtcbin`. MoQ gives none of
   that for free — this spike sends a fixed 1500 kbit/s. That machinery would have
   to be rebuilt.
3. **Single subscriber.** MoQ's relay-side fan-out (its main draw for Phase 5) is
   untested here.

### Next step to make it decisive

Add artificial delay+loss on the relay path and re-measure both transports:

```
sudo tc qdisc add dev lo root netem delay 25ms loss 1%   # then run.sh; undo with `tc qdisc del dev lo root`
```

and build the equivalent `webrtcbin`-loopback probe for an apples-to-apples
number under the same netem profile.

## Setup cost (itself a finding)

- Cloned `github.com/moq-dev/moq`, `cargo build -p moq-relay -p moq-gst` (one Rust
  build, links system GStreamer 1.22+ — same deps Vantage already needs).
- Relay runs from `localhost.toml` with `tls.generate = ["localhost"]` — **no
  openssl/cert steps** for local dev. Speaks raw QUIC (negotiated `moq-lite-04`).
- The plugin `.so` is discovered via `GST_PLUGIN_PATH_1_0`.

MoQ is still pre-IETF-final and `moq-gst`/`moq-net` see breaking changes; pin
versions if this ever graduates past a spike.

## Run it

```bash
cd spikes/moq
./run.sh            # clones+builds moq into ./.moq, starts relay, runs probe
# or point at an existing checkout:
MOQ_DIR=~/src/moq SECS=20 ./run.sh
```

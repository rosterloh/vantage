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

## How latency is measured

Both probes run publisher + subscriber **in one process** and timestamp each frame
at the post-encode boundary and again pre-decode, so they report the **transport
contribution only** (encode/decode excluded — identical for both):

- `src/main.rs` (`moq-lat`): H.264 AU at `moqsink` → `moqsrc`, matched by PTS.
- `src/bin/webrtc-lat.rs` (`webrtc-lat`): two `webrtcbin`s, in-process SDP/ICE,
  matched by **RTP timestamp** (webrtcbin re-times PTS on receive). The match point
  is `webrtcbin`'s output — so it **includes the jitter buffer**, which is part of
  how WebRTC delivers media.

Network conditions are emulated with `netem` on the host loopback. Note the
topology asymmetry, which is the whole point: **MoQ media crosses the relay (2 lo
hops); WebRTC is direct P2P (1 hop).**

## Results — `./compare.sh`, 10 s/run, single subscriber, 1500 kbit/s H.264

| netem on `lo`        | transport | p50 | p90 | p99 | max | delivered |
|----------------------|-----------|----:|----:|----:|----:|-----------|
| **clean**            | MoQ       | 0.8 | 1.0 | 1.4 | 25  | 300/300 |
|                      | WebRTC    | 28.9| 31.6| 32.3| 33  | 299/300 |
| **25ms delay, 1% loss** | MoQ    | 51.1| 51.4|**340**|**431**| 299/300 |
|                      | WebRTC    | 54.8| 56.8| 57.4| 58  | 296/300 |
| **5% loss**          | MoQ       | 0.8 | 29.3| 31.6| 63  | 300/300 |
|                      | WebRTC    | 29.4| 60.6|**406**|**413**| 281/300 |
| **50ms delay**       | MoQ       |101.0|101.5|**742**|830| 297/300 |
|                      | WebRTC    | 78.9| 81.6| 82.3| 87  | 298/300 |

(all latencies ms)

### What this shows

1. **The relay costs a hop.** Under N ms one-way delay, MoQ's floor is **2×N**
   (50ms→101, 25ms→51) because media goes endpoint→relay→endpoint; WebRTC's floor
   is **1×N** (50ms→79, 25ms→55) via a direct hop. On a LAN where the relay is
   off-site, this is the dominant cost and it has no MoQ workaround short of
   colocating a relay.

2. **Jitter buffer vs no buffer.** On a clean link MoQ delivers in ~0.8ms while
   `webrtcbin` adds ~29ms — but that buffer is *why* WebRTC stays tight under loss.

3. **Loss is where they diverge hardest.** QUIC is reliable+ordered, so MoQ
   delivers everything (300/300 even at 5% loss) **but a lost packet stalls the
   stream waiting for retransmit** → p99 blows out to 340–740ms (head-of-line).
   WebRTC drops/conceals late frames (down to 281/300) to **keep latency bounded**
   (tight p99 under delay+loss). For live teleop, bounded latency usually beats
   guaranteed delivery — advantage WebRTC, and MoQ would need app-level
   frame-dropping to compete.

## What this still does NOT cover

- **No congestion control / adaptive bitrate.** Vantage's robot already runs
  transport-cc + `rtpgccbwe` (300–2500 kbit/s) via `webrtcbin`; MoQ gives none of
  that for free (this spike is fixed 1500 kbit/s). On a degrading link WebRTC backs
  off; MoQ here would just keep overshooting.
- **Single subscriber** — MoQ's relay-side fan-out (its main Phase 5 draw) is not
  tested; this measures the last-hop latency tradeoff, where WebRTC is favoured.
- Loopback RTT is symmetric/clean vs a real relay's geography and cross-traffic.

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
./run.sh            # clones+builds moq into ./.moq, starts relay, runs the MoQ probe
./compare.sh        # full MoQ-vs-WebRTC netem matrix (needs docker for unprivileged netem on lo)
# or point at an existing checkout:
MOQ_DIR=~/src/moq SECS=20 ./run.sh
```

Probes: `moq-lat` (MoQ) and `webrtc-lat` (webrtcbin baseline) — `cargo run --release --bin <name>`.

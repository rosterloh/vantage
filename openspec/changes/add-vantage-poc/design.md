# Design: Vantage PoC

## 1. Architecture overview

Three runtime components and two shared libraries:

```
                       ┌─────────────────────┐
                       │  vantage-coordinator │  registry · discovery · signalling
                       │      (axum)          │  relay · fleet stats · TURN creds
                       └──────────┬───────────┘
              register/heartbeat  │  discover / signalling (WebSocket)
            ┌──────────────────────┴───────────────────────┐
            │                                               │
   ┌────────┴─────────┐                          ┌──────────┴────────┐
   │   vantage-robot   │   WebRTC peer connection │   vantage-client   │
   │  GStreamer owns   │ ───────────────────────► │  gstreamer-rs +    │
   │  the camera       │   video (sendonly)       │  webrtcbin receive │
   │  webrtcbin send   │ ◄──────────────────────► │  Slint UI          │
   │  ROS2 raw Image   │   data channel (bidi)    │                    │
   └──────────────────┘                          └───────────────────┘
```

The coordinator is the only always-on internet-reachable component. Media and the
data channel flow peer-to-peer over the WebRTC connection; the coordinator only
brokers discovery and the SDP/ICE exchange and observes session lifecycle for stats.

## 2. Decision: GStreamer owns the camera device

The camera is a shared resource: the WebRTC stream and ROS2 consumers both need
frames. The streamer owns `/dev/video0` (or the CSI source on Jetson) in a single
GStreamer pipeline and fans frames out with a `tee`. Because GStreamer buffers are
refcounted, the `tee` hands out references rather than copies, so fan-out is cheap.

**Consequence:** ROS2 no longer owns the camera, so Vantage must publish
`camera_info` itself for consumers (calibration, `image_proc`) that expect it.

**Alternative considered:** ROS2 owns the camera and the streamer subscribes. This
is the more conventional robot layout and gives raw `Image` for free, but it was
rejected for the PoC because the stream is the first-class workload and a single
hardware-accelerated capture→encode→`webrtcbin` pipeline gives the lowest latency.
If raw `Image` becomes the primary consumer, ownership should flip.

## 3. Decision: raw Image is mandatory → tee sits pre-encode

Raw `sensor_msgs/Image` MUST be available to ROS2. An H.264 branch cannot satisfy
that, so the `tee` is placed before the encoder. On Jetson this means the raw branch
crosses NVMM→system memory via `nvvidconv` and pays one copy; the WebRTC branch can
stay in NVMM. Spending the copy on the (occasional) raw consumer rather than the hot
stream path is the intended trade-off. `CompressedImage` (JPEG) is optional and, if
needed, comes from `image_transport` lazily or a hardware `nvjpegenc` branch — not
from the H.264 stream (a format mismatch).

## 4. Decision: native single-language Rust client

The client uses `gstreamer-rs` + `webrtcbin` for receive and hardware decode, with
decoded frames rendered into Slint. This keeps the whole system in one language and
one media framework, enabling a shared `vantage-protocol` crate with no schema drift.

**Why not a webview:** a webview client would get browser WebRTC "for free", but on
Linux (WebKitGTK, used by Wry/Tauri) WebRTC is experimental and unreliable, often
requiring a custom WebKitGTK build. Betting a teleop console on it is too risky.
**Why not Dioxus-web/WASM:** viable and Rust, but runs in a browser and the
`web-sys` WebRTC layer is awkward; the native path is more aligned with the team's
systems-Rust strength and reuses the GStreamer knowledge from the robot side.

**Frame→texture path:** for the PoC, `appsink` → system memory →
`slint::Image::from_rgba8` per frame (YUV→RGB in a shader). Zero-copy DMABUF/VAAPI
import into a GPU texture is a later optimization if upload cost shows up.

## 5. Decision: media topology — one-way video, two-way connection

Video is a `sendonly` transceiver from the robot (`recvonly` at the client); there
is no client→robot video track, which keeps the SDP simple. The peer connection
carries bidirectional data channels from day one: telemetry flows down now; the
teleop control channel is reserved and flows up later. Establishing both channels
up front avoids renegotiation when control lands.

## 6. Decision: network path selection via ICE

Optimal-path selection is delegated to ICE, not built. Candidates are tried in
priority order: host (direct LAN) → server-reflexive (direct over NAT, via STUN) →
relay (TURN). Because both peers are native code, they advertise real host
candidates (no mDNS `.local` obfuscation), guaranteeing the LAN path is used when
available. STUN servers are public; TURN is metered.ca free tier with static
credentials for the PoC (ephemeral credentials deferred). ICE's "optimal" is
priority-and-reachability, not measured RTT — acceptable for the PoC.

## 7. Decision: codec and serialization

- **Codec:** H.264 — universal hardware encode and decode, low-latency-friendly.
  A later swap (VP8/AV1) touches the payloader, depayloader, and decoder as well as
  the encoder, but the RTP caps contract localizes the change.
- **Serialization:** `serde_json` during bring-up so data-channel traffic is
  readable in logs; swap to `bincode` once high-rate joint states arrive. The shared
  crate's derives make this a one-line codec change.

## 8. Robot pipeline

```
v4l2src / nvarguscamerasrc
  ! tee name=t
    t. ! queue leaky=downstream ! <hw-encoder> ! rtph264pay ! rtp-tee
         rtp-tee ! webrtcbin (consumer 1)
         rtp-tee ! webrtcbin (consumer 2) ...      # encode once, fan out
    t. ! queue ! nvvidconv ! video/x-raw ! appsink # raw → ROS2 Image (+ camera_info)
```

- **Per-branch `queue`** decouples threads so a slow consumer or the encoder cannot
  stall the camera or the ROS2 branch. The WebRTC branch is `leaky=downstream` to
  drop frames rather than build latency.
- **Encoder factory:** probe the registry and select the first available of
  `nvv4l2h264enc` (Jetson) → `nvh264enc` (desktop NVIDIA) → `vah264enc`/`vaapih264enc`
  (Intel/AMD) → `qsvh264enc` → `vtenc_h264` (macOS) → `x264enc` (software fallback).
  The output caps contract is identical regardless of which is selected.
- **Demand-driven branches:** capture+encode stay stable once running; the cheap
  *sink* branches (`webrtcbin` per viewer, `appsink` for ROS2) are added/removed at
  runtime via blocking pad probes — WebRTC on a coordinator connect event, ROS2 on
  subscriber count. Tearing down capture/encode at zero consumers is a later power
  optimization.
- **Keyframe on join:** each new `webrtcbin` consumer triggers a `GstForceKeyUnit`
  upstream so the new viewer can decode immediately; a periodic IDR is the backstop.
- **Congestion control:** `transport-cc` bandwidth estimation drives adaptive
  bitrate, important on the relayed/remote path.

## 9. Coordinator

axum service exposing:
- robot registration + heartbeat; stale entries expire.
- client discovery (the robot list).
- signalling relay: forwards SDP offer/answer and ICE candidates between a chosen
  robot and a connecting client.
- fleet statistics: providers online and consumers connected, derived from session
  lifecycle.
- TURN credential provisioning (static for the PoC).
- mDNS is an optional LAN-local fast path / offline fallback for discovery.

## 10. Crate layout

```
vantage/                     (cargo workspace)
├── vantage-protocol/   lib  — shared serde types (signalling, telemetry, control)
├── vantage-signalling/ lib  — drive webrtcbin + coordinator WS client (robot+client)
├── vantage-coordinator/ bin — registry, discovery, signalling relay, fleet stats
├── vantage-robot/      bin  — GStreamer pipeline, encoder factory, ROS2 raw branch,
│                              telemetry producer (sysinfo)
└── vantage-client/     bin  — Slint UI + webrtcbin receive/decode → texture
```
`vantage-protocol` and `vantage-signalling` may start merged and split later, but
the `protocol` boundary stays clean from day one — shared types are the whole point.

## 11. Open risks and future work
- **Teleop failsafe** (before control lands): the robot MUST detect data-channel /
  connection loss and enter a safe state autonomously (watchdog). Safety-relevant on
  a humanoid; specify before implementing control.
- **Frame→texture cost** on the client is the main technical unknown; prototype
  early against `videotestsrc` (see tasks).
- **`rclrs` maturity:** verify it exposes subscription-count / matched events and
  loaned messages; fall back to coordinator-mediated interest or in-process
  consumers if not.
- **Viewer fan-out** is bounded by robot uplink (robot-as-mini-SFU); a cloud SFU is
  the path if many simultaneous viewers per robot is ever a goal.

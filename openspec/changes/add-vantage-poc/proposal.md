# Add Vantage PoC

## Why
Operators need to see a robot's live onboard camera and its system telemetry
from anywhere, and to know across a fleet how many robots are streaming and how
many operators are watching. Common approaches fall short: a streamer that opens
the camera device exclusively locks out ROS2 consumers; HTTP/TCP streaming builds
latency on unreliable links and cannot do congestion control; and reaching a robot
remotely usually means a VPN or an unencrypted cloud proxy.

Vantage provides a low-latency WebRTC video stream plus a bidirectional data
channel, automatic best-path selection (LAN-direct when possible, relayed only when
necessary), and a central coordinator for discovery and fleet visibility — all in a
single-language Rust stack so that message types are shared end to end with no drift.

## What Changes
- **ADD `vantage-robot`** — robot-side streaming server. Owns the camera via a
  GStreamer pipeline, fans frames to a WebRTC video track and to a raw
  `sensor_msgs/Image` ROS2 branch, selects a hardware encoder at runtime, and
  produces device telemetry over the data channel.
- **ADD `vantage-coordinator`** — rendezvous service. Robot registration and
  heartbeat, client discovery, WebRTC signalling relay (SDP/ICE), fleet statistics
  (providers online, consumers connected), and TURN credential provisioning.
- **ADD `vantage-client`** — native Slint operator console. Discovers robots,
  connects, and shows the live stream beside live telemetry.
- **ADD `vantage-protocol`** — shared `serde` types: signalling messages, telemetry
  messages, and (reserved) control messages. Used by all three binaries.
- **ADD `vantage-signalling`** — shared helper for driving a `webrtcbin` and
  exchanging SDP/ICE with the coordinator. Used by robot and client.

## Scope
**In scope (PoC):** one-way video (robot→operator); bidirectional data channels
(telemetry now, teleop channel reserved); automatic ICE path selection
(host → server-reflexive via STUN → relay via TURN); demand-driven streaming
(encode/branches spin up on connect); encode-once fan-out to multiple consumers;
fleet counts; H.264; static TURN credentials.

**Out of scope (deferred):** teleoperation control logic (channel reserved, with a
disconnect failsafe to be specified before it lands); authentication/authorization
beyond a basic gate; recording; audio; ephemeral TURN credentials; a cloud SFU for
large viewer fan-out; the ROS bridge for joint states (telemetry starts with device
metrics only).

## Impact
- New Cargo workspace with five crates (`vantage-protocol`, `vantage-signalling`,
  `vantage-robot`, `vantage-coordinator`, `vantage-client`).
- New runtime dependency on a STUN server (public) and a TURN relay
  (metered.ca free tier for the PoC).
- The robot takes ownership of the camera device and therefore must publish
  `camera_info` itself for ROS2 consumers that expect it.

## Key Decisions
Summarised here; full rationale in `design.md`.
1. GStreamer owns the camera device; raw `sensor_msgs/Image` is a hard requirement.
2. Native single-language Rust client (`gstreamer-rs` + `webrtcbin` + Slint), not a
   webview — Linux webview WebRTC (WebKitGTK) is unreliable.
3. ICE performs optimal-path selection; the coordinator performs discovery,
   signalling, and fleet stats.
4. Encode once and fan the encoded RTP out to N consumers; add/remove sink branches
   on demand.
5. One-way video, two-way connection: a `sendonly` video transceiver plus
   bidirectional data channels established from day one.

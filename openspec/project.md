# Project: Vantage

## Purpose
Vantage lets an operator see a robot's live onboard camera and system telemetry
from anywhere — same LAN or remote — and gives a fleet-wide view of how many
robots are streaming and how many operators are connected. It is built as a
low-latency WebRTC system with a single-language (Rust) stack.

## Conventions
- Language: Rust across the entire stack (robot, coordinator, client, shared crates).
- Workspace: single Cargo workspace; crates namespaced `vantage-*` (the bare
  `vantage` crate name is taken on crates.io, so nothing is published unprefixed).
- Media framework: GStreamer via `gstreamer-rs` on both the robot and the client.
- WebRTC: `webrtcbin` on both ends; ICE for path selection.
- Serialization on the data channel: `serde_json` during bring-up (human-readable,
  debuggable), with a one-line swap to `bincode` once high-rate data arrives.
- Specs are the source of truth; behavioural requirements use GIVEN/WHEN/THEN.

## Non-negotiables
- Raw `sensor_msgs/Image` MUST remain available to the ROS2 graph. The streamer
  must never lock the camera away from other on-robot consumers.
- The robot must not depend on a specific GPU vendor; hardware encode is selected
  at runtime with a software fallback.

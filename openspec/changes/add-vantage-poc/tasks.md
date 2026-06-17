# Tasks: Vantage PoC

Build order front-loads the two riskiest pieces — network traversal and the
client decode→texture path — and defers the laborious-but-understood parts
(real camera, hardware accel, dynamic branches). Steps 2 and 3 are the ones to
be patient with; if both work, the rest is mostly assembly.

## 1. Protocol skeleton
- [ ] Create the Cargo workspace and the five `vantage-*` crates.
- [ ] In `vantage-protocol`, define signalling message types (register, robot list,
      SDP offer/answer, ICE candidate) and `DeviceInfo` telemetry (cpu, mem, temps).
- [ ] Choose `serde_json` as the initial data-channel codec behind a small wrapper.

## 2. Signalling spine — no media
- [ ] `vantage-coordinator`: robot registration + heartbeat, client discovery,
      SDP/ICE relay over WebSocket.
- [ ] `vantage-robot` and `vantage-client`: establish a WebRTC peer connection with
      **only a data channel** (no video track).
- [ ] Send `DeviceInfo` (via `sysinfo`) from robot to client over the data channel.
- [ ] **Test the TURN relay path explicitly**, not just same-LAN, to prove ICE
      traversal and the static TURN credentials.
- [ ] Exit criteria: a robot and a client find each other through the coordinator
      and exchange telemetry over WebRTC, including when forced onto the relay.

## 3. Video with `videotestsrc` + `x264enc`
- [ ] Robot: add a `webrtcbin` video branch fed by `videotestsrc ! x264enc`
      (software, no camera, no hardware drivers).
- [ ] Client: receive, decode, and render the stream into Slint via
      `appsink` → `slint::Image::from_rgba8`.
- [ ] Exit criteria: a test pattern renders in the Slint window beside live
      telemetry. (This isolates webrtcbin negotiation and the texture path — the
      main client risk — from camera/driver variables.)

## 4. Real camera + tee + encoder factory
- [ ] Swap source to `v4l2src` / `nvarguscamerasrc`.
- [ ] Add the `tee`: WebRTC branch + raw `appsink` → ROS2 `sensor_msgs/Image`
      (via `rclrs`), and publish `camera_info`.
- [ ] Implement the encoder factory (runtime selection with `x264enc` fallback).
- [ ] Exit criteria: live camera renders on the client AND raw `Image` is visible
      on the ROS2 graph simultaneously.

## 5. Hardware decode, dynamic branches, multi-consumer
- [ ] Client-side hardware decode (VAAPI/NVDEC).
- [ ] Encode-once fan-out: RTP `tee` feeding one `webrtcbin` per consumer.
- [ ] Demand-driven sink branches (add/remove via blocking pad probes) — WebRTC on
      connect event, ROS2 on subscriber count.
- [ ] `GstForceKeyUnit` on each new viewer; `transport-cc` adaptive bitrate.
- [ ] Exit criteria: two clients view one robot; a new joiner gets video instantly;
      bitrate adapts under simulated loss.

## 6. Fleet stats, LAN fast-path, then teleop groundwork
- [ ] Coordinator tracks sessions; expose providers-online / consumers-connected.
- [ ] mDNS LAN-local discovery fast path.
- [ ] Reserve and wire the bidirectional control data channel.
- [ ] **Specify and implement the teleop disconnect failsafe/watchdog before any
      control command is acted on.**
- [ ] (Later) Bridge ROS topics (joint states) into telemetry; swap codec to
      `bincode` for high-rate data.

# Vantage Phase 4b — ROS 2 Bridge (camera → `sensor_msgs/Image`) Design

**Status:** Approved. Implementation plan: `docs/superpowers/plans/2026-06-21-vantage-phase4b-ros2-bridge.md`.
**Date:** 2026-06-21
**Builds on:** Phase 4a (`docs/superpowers/plans/2026-06-19-vantage-phase4a-camera-tee-encoder.md`),
merged to `main`. The robot's pre-encode `tee` already exposes raw RGB frames via
`Peer::recv_raw_frame() -> Option<RawFrame>` (`vantage-signalling/src/peer.rs`), where
`RawFrame { width: u32, height: u32, encoding: String /* "rgb8" */, data: Vec<u8> /* packed w*h*3 */ }`.

## Goal

Publish the camera frames already flowing out of the pre-encode tee to the ROS 2 graph as
`sensor_msgs/Image` + `sensor_msgs/CameraInfo`, **concurrently** with the WebRTC stream, so the
camera is shared (not monopolised). This completes the camera-sharing spec's hard requirements
"Raw image availability" and "Camera info published". Optional `CompressedImage` is **deferred**.

## Spec coverage

- camera-sharing **"Raw image availability"** (hard) → `sensor_msgs/Image` on `~/image_raw`.
- camera-sharing **"Camera info published"** → `sensor_msgs/CameraInfo` on `~/camera_info`.
- camera-sharing **"Camera not monopolised"** → already proven by 4a's concurrent tee; 4b's
  publisher consumes the existing raw branch, so the WebRTC stream is unaffected.
- camera-sharing **"Optional compressed image"** → explicitly **out of scope** for 4b (see Deferred).

## Environment & toolchain

- **Distro:** native ROS 2 **Lyrical** (Lyrical Luth) on Ubuntu 26.04, `/opt/ros/lyrical`.
  Decision rationale and rejected alternatives (pixi/Robostack) recorded in project memory
  `vantage-phase4b-ros2-bridge`.
- **Sourcing:** all ROS-enabled `cargo` invocations MUST `source /opt/ros/lyrical/setup.zsh`
  (zsh-native; `setup.bash` silently no-ops under zsh → `AMENT_PREFIX_PATH` unset → build panic).
- **`rclrs` Lyrical support:** not yet upstream. Tracked by
  [ros2-rust/ros2_rust#640](https://github.com/ros2-rust/ros2_rust/pull/640) ("add lyrical to CI
  jobs and rcl_bindings.rs"), expected to land soon. Until then a local shim maps `lyrical → rolling`
  bindings (`build.rs KNOWN_DISTROS += "lyrical"`, `rcl_bindings.rs` reuses rolling bindings).
- **Vendoring (for CI reproducibility):** the ROS build MUST NOT reference the developer's
  `~/ros2_ws`. Vendor `rclrs` (the Lyrical-patched source) into the repo as a **git submodule**
  pinned to a fork/branch carrying the 2-line lyrical patch, to be **repointed at #640** once merged.
  The ROS-build `.cargo/config.toml` patches `rclrs` → the submodule and the message crates →
  `/opt/ros/lyrical/share/*/rust` (stable inside the container).
- **rmw:** stay **rmw-agnostic** — do not set `RMW_IMPLEMENTATION` in code; default Fast-RTPS.
  The user's usual `rmw_zenoh_cpp` remains selectable at runtime via env. (A Zenoh tooling
  simplification is a possible later phase, not 4b.)

## Architecture — in-process, feature-gated

`vantage-robot` links `rclrs` **directly**, behind a default-off cargo feature, so the default
`cargo build/test --workspace` stays ROS-free (preserving the README's "protocol + coordinator
build without system deps" property). A later split into a separate `vantage-ros-bridge` process
stays cheap because the conversion logic is pure and the publish boundary is thin.

### Module layout (`vantage-robot/src/`)

```
main.rs        # feature-gated drain selection at the existing wiring point (main.rs:55-72)
ros/
  mod.rs       # #[cfg(feature = "ros")] node, publishers, publish loop
  convert.rs   # PURE, ROS-FREE: RawFrame -> ImageParts (+ CameraInfoParts). Unit-tested.
```

- `convert.rs` is compiled and tested **unconditionally** (no `rclrs` types). It owns the byte/layout
  logic so it runs in the fast CI lane.
- `ros/mod.rs` is `#[cfg(feature = "ros")]` and is the only place `rclrs`/`sensor_msgs` appear.

### Conversion contract (pure, ROS-free)

```rust
/// Plain, dependency-free description of an Image message body.
pub struct ImageParts {
    pub height: u32,
    pub width: u32,
    pub encoding: String,   // "rgb8"
    pub is_bigendian: u8,   // 0
    pub step: u32,          // width * 3
    pub data: Vec<u8>,      // moved from RawFrame.data
}

/// Minimal CameraInfo body. No real calibration in 4b (streamer owns the device); width/height and
/// header must match the image. distortion_model = "plumb_bob", d/k/r/p zeroed (placeholder).
pub struct CameraInfoParts {
    pub height: u32,
    pub width: u32,
    pub distortion_model: String, // "plumb_bob"
}

pub fn image_parts(frame: RawFrame) -> ImageParts;       // step = width*3, encoding passthrough
pub fn camera_info_parts(frame: &RawFrame) -> CameraInfoParts;
```

`ros/mod.rs` maps `ImageParts`/`CameraInfoParts` → `sensor_msgs::msg::{Image, CameraInfo}`, stamping
`header.stamp` from the node clock and `header.frame_id` from `VANTAGE_CAMERA_FRAME_ID`
(default `"camera_optical_frame"`).

### Wiring (`main.rs`, the `ClientConnected` arm)

The raw branch has a single consumer (`recv_raw_frame` locks one receiver). Feature-gate which drain
owns it:

- `#[cfg(feature = "ros")]` → spawn the ROS publish task (consumes `recv_raw_frame`, publishes
  Image + CameraInfo, may also log counts).
- `#[cfg(not(feature = "ros"))]` → the existing log-only drain (4a behaviour) is unchanged.

### Cargo features (`vantage-robot/Cargo.toml`)

```toml
[features]
ros = ["dep:rclrs", "dep:sensor_msgs"]

[dependencies]
rclrs = { version = "0.7", optional = true }
sensor_msgs = { version = "*", optional = true }   # patched to /opt/ros/lyrical native crate in ROS build
```

## Topics & messages

| Topic                 | Type                       | Notes |
|-----------------------|----------------------------|-------|
| `~/image_raw`         | `sensor_msgs/Image`        | rgb8, `step = width*3`, default QoS (sensor-data QoS optional) |
| `~/camera_info`       | `sensor_msgs/CameraInfo`   | width/height + header match the image; placeholder calibration |

Node name `vantage_camera` (configurable). Topics relative so they can be remapped/namespaced.

## Testing strategy

- **Fast lane (no ROS, default features):** unit tests on `convert.rs` — `image_parts` sets
  `step == width*3`, `encoding == "rgb8"`, `data.len() == width*height*3`; `camera_info_parts`
  matches dimensions. `cargo test --workspace` stays green with zero ROS deps.
- **ROS lane (Lyrical container):** build `-p vantage-robot --features ros`; run coordinator + robot
  (+ a client to drive the WebRTC stream); assert via `ros2 topic list` / `ros2 topic echo --once`
  that `image_raw` (rgb8, correct dims) and `camera_info` are published, **while** the client still
  decodes video (concurrency, mirroring the 4a evidence).

## CI — two lanes

1. **Default lane** (existing): `cargo test --workspace` with no ROS/`ros` feature — fast, unchanged.
2. **ROS lane** (new): `FROM ubuntu:26.04` + `apt install ros-lyrical-*`, source `setup.zsh`,
   `cargo build -p vantage-robot --features ros`, run the integration check above.

## Deferred / out of scope

- `sensor_msgs/CompressedImage` (optional in spec) — separate JPEG branch later, never derived from
  the H.264 track.
- Real camera calibration (CameraInfo `k`/`p`/`d`) — placeholder for now.
- Splitting the bridge into its own process — revisit if the in-process ROS coupling becomes a problem.
- Hardware decode, adaptive bitrate, keyframe-on-join — Phase 5.

## Exit criteria

- [ ] `cargo test --workspace` (no ROS) green, including the `convert.rs` unit tests.
- [ ] In the Lyrical container, `vantage-robot --features ros` publishes `image_raw` (rgb8, correct
      dimensions) and `camera_info` to the ROS graph.
- [ ] Those publish **concurrently** with the WebRTC stream (client still decodes video) — camera
      not monopolised, end to end.
- [ ] The `ros` feature is off by default; default build/test pulls in no ROS dependency.

## Open risks

- **`lyrical → rolling` binding drift:** correct at Lyrical's release (forked from Rolling) but
  Rolling moves; replace with #640's generated lyrical bindings when it merges.
- **Submodule vs #640 timing:** if #640 lands before implementation, skip the local shim and consume
  upstream directly (still vendor/pin for CI).

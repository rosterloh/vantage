# Phase 4b (ROS 2 bridge: camera → sensor_msgs/Image) — Exit Evidence (2026-06-21)

Plan: `docs/superpowers/plans/2026-06-21-vantage-phase4b-ros2-bridge.md`
Branch: `vantage-phase4b-ros-bridge`. ROS 2 Lyrical, rclrs 0.7.0 (local `~/ros2_ws` shim at
upstream `9abcd5d` #641 + 2-line lyrical→rolling patch). GStreamer test-source pipeline.

## Architecture as built ("Option C")

The spec's in-process feature design hit a hard cargo constraint: `sensor_msgs` has no usable
crates.io release, and cargo resolves *disabled* optional deps into the shared lock — so an
optional `sensor_msgs` dep breaks `cargo build --workspace` even with `ros` off. `rclrs` 0.7.0
*is* on crates.io. Resolved (user-approved) by an **always-on** root `.cargo/config.toml`
patching `rclrs` → the `~/ros2_ws` shim and the message crates → `/opt/ros/lyrical/share/*/rust`.
Consequence (tracked debt): the default workspace build no longer compiles any ROS code but now
requires the Lyrical paths at *resolve* time. The submodule + Dockerfile + two-lane CI from the
plan are **deferred** (revisit when ros2_rust#640 publishes Lyrical crates).

## Automated tests — `cargo test --workspace` ✅

All green, including the two new pure `convert` tests
(`image_parts_sets_step_and_preserves_encoding_and_data`, `camera_info_matches_image_dimensions`)
and the pre-existing suites. Default lane compiles **zero** ROS code.

## Feature build — `cargo build -p vantage-robot --features ros` ✅

Builds natively against Lyrical (ROS sourced via `setup.zsh`): `rclrs` + `sensor_msgs` +
message crates compile; vantage-robot links clean (only warnings originate inside `rclrs`).

## ROS graph ✅

Coordinator + robot (`--features ros`) running:

```
$ ros2 node list
/vantage_camera
$ ros2 topic info /vantage_camera/image_raw
Type: sensor_msgs/msg/Image      Publisher count: 1
$ ros2 topic info /vantage_camera/camera_info
Type: sensor_msgs/msg/CameraInfo  Publisher count: 1
```

Node name `vantage_camera`; relative topics `~/image_raw`, `~/camera_info`.

## Core exit criteria — raw image availability + camera not monopolised ✅

Coordinator + robot (`--features ros`, FastRTPS) + headless client (test-source pipeline):

```
robot : published ros image 640x480 (#12330) … (#12510)   ~30 fps   sensor_msgs/Image
client: video frame        640x480 (#12390) … (#12480)   ~30 fps   WebRTC decode
```

The ROS publisher and the WebRTC stream run **simultaneously from the one pre-encode tee** at the
same frame indices/timestamps — the camera is shared, not monopolised. This satisfies the
camera-sharing hard requirements "Raw image availability" and "Camera not monopolised", end to end.

## Known issue — subscriber-delivery SIGSEGV (native shim, NOT Phase 4b code)

The robot **SIGSEGVs (exit 139) at the point of actual message serialization to a matched reader**:

- **FastRTPS:** published 12,480 frames fine with no subscriber, then crashed the instant
  `ros2 topic echo` matched (no reader ⇒ no serialization, so the no-subscriber path was cheap).
- **rmw_zenoh_cpp:** crashed on the *first* publish (0 frames logged) — Zenoh serializes
  immediately, so the same serialization path is hit at once.

Reproduced **with** the executor spin thread, **without** it, and with the executor dropped, and
under **both** rmw implementations ⇒ the fault is **rmw-independent**, in the native
rcl/rosidl typesupport path, not in vantage Rust code (which constructs valid messages). This is
the spec's documented open risk *"lyrical → rolling binding drift"*: the `~/ros2_ws` tree sits at
#641 *"handle rolling introspection member layout"*, and serializing the `image.data` sequence
through that introspection layout is what faults.

Because the publisher dies on subscriber match, the `ros2 topic echo` payload (live rgb8/step
bytes) could not be captured; the robot's own logs confirm 640×480 rgb8 (`convert` sets
`step = width*3`, unit-tested).

**Lead for resolution (out of Phase 4b scope):** regenerate the `/opt/ros/lyrical` `sensor_msgs`
Rust crate from the current #641 generator, or adopt ros2_rust#640 once it lands; then re-run the
subscriber-delivery test.

## Status

- Tasks 1–3 complete and review-clean; publish-side Task 4 proven above.
- Deferred: subscriber-delivery (blocked by the shim issue above), Docker/CI two-lane harness,
  and a fully ROS-free default lane (all tracked).

---

## Addendum 2026-06-26 — switched rclrs → r2r; SIGSEGV blocker resolved

The "Option C" rclrs arrangement above was **replaced with `r2r`** before merge (the simpler tooling
won out for a publish-only bridge). `r2r` generates rcl + message bindings at build time from the
sourced ROS env, so the always-on `.cargo/config.toml` patch, the `~/ros2_ws` local shim, and the
`sensor_msgs`/`rclrs` crates.io deps are all **gone**. `vantage-robot/src/ros/mod.rs` now uses
`r2r` (git-pinned to rev `2d080c4` = r2r 0.9.6-dev, since crates.io 0.9.5 predates Lyrical and
rejects `ROS_DISTRO=lyrical`); `convert.rs`/`main.rs` unchanged. Rationale + tradeoffs in project
memory `vantage-r2r-vs-rclrs-tradeoffs`.

**The SIGSEGV "Known issue" above is resolved**, not deferred. r2r links native `rcl` directly,
bypassing the rolling-introspection typesupport path that faulted. Re-verified 2026-06-26 with a
temporary standalone driver (since reverted):

```
$ ros2 topic hz /vantage_camera/image_raw     # matched a real subscriber — the path that SIGSEGV'd
average rate: 29.716   (image_raw)
average rate: 29.818   (camera_info)
$ ros2 topic echo --once /vantage_camera/image_raw --field step   → 1920   (= width*3 ✓)
                                                   --field encoding → rgb8
                                                   --field width/height → 640 / 480
```

Ran 142k+ frames overnight across subscriber connect/disconnect with **zero segfault/panic**.

**Side effect:** dropping the cargo patch restores the fully ROS-free default lane —
`cargo build/test --workspace` no longer needs `/opt/ros/lyrical` at resolve time (r2r's build.rs
only runs under `--features ros`). That deferred item is now also closed.

**Still genuinely deferred:** live WebRTC→`recv_raw_frame()` end-to-end on hardware (this addendum
used synthetic frames; the frame-production side is unchanged from 4a), and the Docker/CI two-lane
harness.

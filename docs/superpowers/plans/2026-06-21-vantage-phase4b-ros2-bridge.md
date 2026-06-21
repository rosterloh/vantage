# Vantage Phase 4b — ROS 2 Bridge (camera → `sensor_msgs/Image`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish the pre-encode tee's raw camera frames to the ROS 2 graph as `sensor_msgs/Image` + `sensor_msgs/CameraInfo`, concurrently with the WebRTC stream, behind a default-off `ros` cargo feature.

**Architecture:** A pure, ROS-free conversion module (`convert.rs`) owns the byte/layout logic and is unit-tested in the default fast lane. A feature-gated `ros/mod.rs` (the only place `rclrs`/`sensor_msgs` appear) maps the pure parts onto ROS messages and publishes them. `main.rs` picks the raw-branch drain at compile time: ROS publisher when `--features ros`, the existing log-only drain otherwise.

**Tech Stack:** Rust (workspace, edition 2021, rust 1.90), tokio, `rclrs` 0.7 + `sensor_msgs` (ROS 2 Lyrical, native), GStreamer (existing, via `vantage-signalling`).

## Global Constraints

Every task's requirements implicitly include this section. Values copied verbatim from the spec.

- **Distro:** native ROS 2 **Lyrical** on Ubuntu 26.04, `/opt/ros/lyrical`.
- **Sourcing (interactive/zsh):** all ROS-enabled `cargo` invocations in the developer's zsh shell MUST `source /opt/ros/lyrical/setup.zsh` (`setup.bash` silently no-ops under zsh → `AMENT_PREFIX_PATH` unset → build panic). Inside the Dockerfile, `RUN` steps run under `bash`, so they `source /opt/ros/lyrical/setup.bash` (correct for bash).
- **rmw:** stay **rmw-agnostic** — never set `RMW_IMPLEMENTATION` in code; default Fast-RTPS, `rmw_zenoh_cpp` selectable at runtime via env.
- **No `~/ros2_ws` reference in the build:** the ROS build MUST NOT reference the developer's `~/ros2_ws`. `rclrs` is vendored as a git submodule (local path for now — see Task 2 — repoint to a fork / PR #640 later).
- **Default stays ROS-free:** `cargo build/test --workspace` (no `ros` feature) MUST pull in zero ROS dependencies; the `ros` feature is off by default.
- **`convert.rs` is pure:** compiled and tested **unconditionally**, contains no `rclrs`/`sensor_msgs` types.
- **Node name** `vantage_camera` (override `VANTAGE_CAMERA_NODE`); topics **relative** (`~/image_raw`, `~/camera_info`) so they remap/namespace.
- **Frame id** from `VANTAGE_CAMERA_FRAME_ID`, default `"camera_optical_frame"`.

**Source of truth for the riskiest step:** the Rust message-crate generation for Lyrical already works in the developer's `~/ros2_ws` (the local `lyrical → rolling` shim). Task 2 **ports that proven recipe** into `ros-build/Dockerfile` rather than reconstructing it.

---

## File Structure

| File | Responsibility | Tasks |
|------|----------------|-------|
| `vantage-robot/src/convert.rs` | **Create.** Pure `RawFrame → ImageParts/CameraInfoParts`. ROS-free, unit-tested. | 1 |
| `vantage-robot/src/main.rs` | **Modify.** Declare `convert` (Task 1) and `ros` (Task 2) modules; feature-gate the raw-branch drain (Task 3). | 1, 2, 3 |
| `vantage-robot/Cargo.toml` | **Modify.** `[features] ros`, optional `rclrs`/`sensor_msgs`. | 2 |
| `vantage-robot/src/ros/mod.rs` | **Create.** `#[cfg(feature = "ros")]` node + publishers + publish mapping. Empty placeholder in Task 2, filled in Task 3. | 2, 3 |
| `.gitmodules` + `third-party/ros2_rust` | **Create.** Vendored Lyrical-patched `rclrs` submodule. | 2 |
| `ros-build/cargo-config.toml` | **Create.** ROS-lane-only `[patch]` (kept out of the default lane). | 2 |
| `ros-build/Dockerfile` | **Create.** ubuntu:26.04 + ROS Lyrical + ported message-gen recipe + cargo build. | 2 |
| `.github/workflows/ci.yml` | **Create.** Two lanes: default (`cargo test --workspace`) + ROS (container build + integration). | 4 |
| `docs/superpowers/plans/notes/2026-06-21-phase4b-exit.md` | **Create.** Exit evidence (topics published concurrently with the stream). | 4 |

---

## Task 1: Pure conversion module (`convert.rs`)

The keystone, fully testable in the default fast lane (no ROS). Owns `step`/encoding/layout so the ROS module stays a thin mapping shell.

**Files:**
- Create: `vantage-robot/src/convert.rs`
- Modify: `vantage-robot/src/main.rs:1` (add module declaration)

**Interfaces:**
- Consumes: `vantage_signalling::peer::RawFrame { width: u32, height: u32, encoding: String, data: Vec<u8> }` (existing, all fields `pub`).
- Produces (relied on by Task 3):
  - `pub struct ImageParts { height: u32, width: u32, encoding: String, is_bigendian: u8, step: u32, data: Vec<u8> }`
  - `pub struct CameraInfoParts { height: u32, width: u32, distortion_model: String }`
  - `pub fn image_parts(frame: RawFrame) -> ImageParts` — **consumes** the frame (moves `data`).
  - `pub fn camera_info_parts(frame: &RawFrame) -> CameraInfoParts` — **borrows** (so a caller can `camera_info_parts(&frame)` then `image_parts(frame)`).

- [ ] **Step 1: Write the failing tests**

Create `vantage-robot/src/convert.rs`:

```rust
//! Pure, ROS-free conversion of pre-encode tee frames into plain message-body
//! descriptions. `ros/mod.rs` maps these onto `sensor_msgs` types; this module
//! never references `rclrs`/`sensor_msgs`, so it compiles and tests in the
//! default (ROS-free) lane.

use vantage_signalling::peer::RawFrame;

/// Plain description of a `sensor_msgs/Image` body.
pub struct ImageParts {
    pub height: u32,
    pub width: u32,
    pub encoding: String, // "rgb8"
    pub is_bigendian: u8, // 0
    pub step: u32,        // width * 3
    pub data: Vec<u8>,    // moved from RawFrame.data
}

/// Minimal `sensor_msgs/CameraInfo` body. No real calibration in 4b — the
/// streamer owns the device; width/height + header must match the image.
pub struct CameraInfoParts {
    pub height: u32,
    pub width: u32,
    pub distortion_model: String, // "plumb_bob"
}

#[cfg(test)]
mod tests {
    use super::*;
    use vantage_signalling::peer::RawFrame;

    fn frame(w: u32, h: u32) -> RawFrame {
        RawFrame {
            width: w,
            height: h,
            encoding: "rgb8".to_string(),
            data: vec![0u8; (w * h * 3) as usize],
        }
    }

    #[test]
    fn image_parts_sets_step_and_preserves_encoding_and_data() {
        let parts = image_parts(frame(4, 2));
        assert_eq!(parts.width, 4);
        assert_eq!(parts.height, 2);
        assert_eq!(parts.step, 12); // width * 3
        assert_eq!(parts.encoding, "rgb8");
        assert_eq!(parts.is_bigendian, 0);
        assert_eq!(parts.data.len(), 4 * 2 * 3);
    }

    #[test]
    fn camera_info_matches_image_dimensions() {
        let info = camera_info_parts(&frame(640, 480));
        assert_eq!(info.width, 640);
        assert_eq!(info.height, 480);
        assert_eq!(info.distortion_model, "plumb_bob");
    }
}
```

Add the module declaration at `vantage-robot/src/main.rs:1`. The functions are unused in the default (non-`ros`) **build** but exercised by tests, so silence dead-code for that configuration:

```rust
mod telemetry;

#[allow(dead_code)] // used by ros/mod.rs under --features ros; exercised by unit tests otherwise
mod convert;
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p vantage-robot convert`
Expected: FAIL — `cannot find function `image_parts`` / `cannot find function `camera_info_parts``.

- [ ] **Step 3: Write the minimal implementation**

Insert into `vantage-robot/src/convert.rs`, after the `CameraInfoParts` struct and before `#[cfg(test)]`:

```rust
/// Convert a raw tee frame into Image body parts. Moves the pixel buffer (no copy).
/// `rgb8` is 3 bytes/pixel, tightly packed, so `step = width * 3`.
pub fn image_parts(frame: RawFrame) -> ImageParts {
    ImageParts {
        height: frame.height,
        width: frame.width,
        is_bigendian: 0,
        step: frame.width * 3,
        encoding: frame.encoding,
        data: frame.data,
    }
}

/// Derive a placeholder `CameraInfo` body from a frame. Borrows (does not consume)
/// so the same frame can be handed to `image_parts` afterwards.
pub fn camera_info_parts(frame: &RawFrame) -> CameraInfoParts {
    CameraInfoParts {
        height: frame.height,
        width: frame.width,
        distortion_model: "plumb_bob".to_string(),
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p vantage-robot convert`
Expected: PASS — both tests green.

- [ ] **Step 5: Verify the whole default lane stays green and ROS-free**

Run: `cargo test --workspace`
Expected: PASS, no new dependencies.
Run: `cargo tree -p vantage-robot | grep -i rcl || echo "no rclrs (good)"`
Expected: prints `no rclrs (good)`.

- [ ] **Step 6: Commit**

```bash
git add vantage-robot/src/convert.rs vantage-robot/src/main.rs
git commit -m "feat(robot): pure RawFrame->ImageParts/CameraInfoParts conversion"
```

---

## Task 2: Cargo feature, vendored `rclrs`, and ROS build toolchain

The infrastructure gate. After this, `cargo build -p vantage-robot --features ros` compiles `rclrs` + the generated `sensor_msgs` crate inside the container (proving the `lyrical → rolling` shim + submodule + message-gen all resolve), while the default lane is unchanged. The `ros` module is an empty placeholder here; Task 3 fills it.

**Files:**
- Modify: `vantage-robot/Cargo.toml`
- Create: `vantage-robot/src/ros/mod.rs` (empty placeholder)
- Modify: `vantage-robot/src/main.rs` (declare the gated module)
- Create: `.gitmodules`, `third-party/ros2_rust` (submodule)
- Create: `ros-build/cargo-config.toml`, `ros-build/Dockerfile`

**Interfaces:**
- Produces: the `ros` cargo feature (`= ["dep:rclrs", "dep:sensor_msgs"]`); a buildable ROS container image; the `crate::ros` module path (empty for now).

- [ ] **Step 1: Declare the feature and optional deps**

Edit `vantage-robot/Cargo.toml` — add a `[features]` table and the two optional deps (verbatim from the spec):

```toml
[features]
ros = ["dep:rclrs", "dep:sensor_msgs"]
```

Append to the existing `[dependencies]` table:

```toml
rclrs = { version = "0.7", optional = true }
sensor_msgs = { version = "*", optional = true } # patched to the generated Lyrical crate in the ROS build
```

- [ ] **Step 2: Verify the default lane still resolves and stays ROS-free**

Run: `cargo tree -p vantage-robot`
Expected: resolves cleanly; `rclrs`/`sensor_msgs` appear only as `(optional)` and are **not** compiled.
Run: `cargo test --workspace`
Expected: PASS (unchanged from Task 1).

> If resolution fails because `sensor_msgs`/`rclrs` are not on crates.io in your registry, do **not** convert them to path deps (path deps must exist at resolve time and would break the default lane). Instead move the `[patch]` from Step 4 into a root `.cargo/config.toml` guarded so the default lane never activates the feature, and record the deviation in the commit message. This is the one place the spec's registry assumption may bite — surface it, don't paper over it.

- [ ] **Step 3: Add the empty gated module + declaration**

Create `vantage-robot/src/ros/mod.rs`:

```rust
//! Feature-gated ROS 2 camera bridge. The ONLY place `rclrs`/`sensor_msgs`
//! appear. Filled in Task 3.
```

Add to `vantage-robot/src/main.rs`, after the `mod convert;` line from Task 1:

```rust
#[cfg(feature = "ros")]
mod ros;
```

- [ ] **Step 4: Vendor `rclrs` as a submodule and write the ROS-only cargo config**

The fork does not exist yet, so vendor from the proven local checkout for now (the `lyrical → rolling` shim already living in `~/ros2_ws/src/ros2_rust`). Repoint to a published fork or PR #640 later.

```bash
git submodule add file:///home/rosterloh/ros2_ws/src/ros2_rust third-party/ros2_rust
git -C third-party/ros2_rust checkout HEAD   # pin to the shim commit currently checked out
git add .gitmodules third-party/ros2_rust
```

Create `ros-build/cargo-config.toml` — kept out of the default lane (applied only via `--config` in the ROS build):

```toml
# ROS-lane-only cargo config. Invoked as:
#   cargo build -p vantage-robot --features ros --config ros-build/cargo-config.toml
# Keeping these [patch] entries here (not in .cargo/config.toml) means the
# default ROS-free lane never sees them.

[patch.crates-io]
# rclrs: built from the vendored Lyrical-shim source by cargo.
rclrs = { path = "third-party/ros2_rust/rclrs" }

# Generated rosidl Rust message crates, installed into the ROS underlay by the
# Dockerfile's message-gen step (see Task 2 Step 5). sensor_msgs pulls in
# std_msgs + builtin_interfaces + geometry_msgs transitively — all must be
# patched to their generated paths.
sensor_msgs        = { path = "/opt/ros/lyrical/share/sensor_msgs/rust" }
std_msgs           = { path = "/opt/ros/lyrical/share/std_msgs/rust" }
builtin_interfaces = { path = "/opt/ros/lyrical/share/builtin_interfaces/rust" }
geometry_msgs      = { path = "/opt/ros/lyrical/share/geometry_msgs/rust" }
```

> If the ROS build later reports another unresolved generated crate (e.g. `sensor_msgs` gains a new interface dep), add it here with the same `/opt/ros/lyrical/share/<pkg>/rust` path — that is a concrete, mechanical fix, not a guess.

- [ ] **Step 5: Write the ROS-build Dockerfile (port the proven `~/ros2_ws` recipe)**

Create `ros-build/Dockerfile`. The message-generation block must be **ported verbatim** from the colcon commands that already produce Rust message crates in `~/ros2_ws` (the local Lyrical shim recorded in memory `rclrs-lyrical-local-shim`). The skeleton below is correct for everything except the exact `colcon build` line — replace the marked line with the one from your working workspace, and ensure it installs into `/opt/ros/lyrical` (so the `cargo-config.toml` paths resolve).

```dockerfile
FROM ubuntu:26.04
ENV DEBIAN_FRONTEND=noninteractive

# ROS 2 Lyrical + colcon + clang (rclrs bindgen) + GStreamer (vantage-signalling).
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl gnupg lsb-release ca-certificates && \
    curl -sSL https://raw.githubusercontent.com/ros/rosdistro/master/ros.key \
      -o /usr/share/keyrings/ros-archive-keyring.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/ros-archive-keyring.gpg] http://packages.ros.org/ros2/ubuntu $(lsb_release -cs) main" \
      > /etc/apt/sources.list.d/ros2.list && \
    apt-get update && apt-get install -y --no-install-recommends \
      ros-lyrical-ros-base ros-lyrical-sensor-msgs \
      python3-colcon-common-extensions python3-rosdep \
      git build-essential libclang-dev clang \
      libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev \
      gstreamer1.0-plugins-good gstreamer1.0-plugins-bad gstreamer1.0-libav && \
    rm -rf /var/lib/apt/lists/*

# Rust toolchain (workspace floor is 1.90).
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.90
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /vantage
COPY . .

# --- Generate Rust message crates into the ROS underlay (PORT FROM ~/ros2_ws) ---
# Must result in /opt/ros/lyrical/share/{sensor_msgs,std_msgs,builtin_interfaces,geometry_msgs}/rust
RUN bash -c 'set -eux; \
    source /opt/ros/lyrical/setup.bash; \
    mkdir -p /overlay/src; \
    cp -r third-party/ros2_rust /overlay/src/ros2_rust; \
    cd /overlay; \
    REPLACE_WITH_PROVEN_RECIPE_FROM_ros2_ws'   # <-- the working colcon line, --install-base /opt/ros/lyrical --merge-install

# --- Build the robot with the ROS feature using the ROS-only cargo config ---
RUN bash -c 'set -eux; \
    source /opt/ros/lyrical/setup.bash; \
    cargo build -p vantage-robot --features ros --config ros-build/cargo-config.toml'
```

- [ ] **Step 6: Build the image to verify the toolchain compiles**

Run: `docker build -f ros-build/Dockerfile -t vantage-ros .`
Expected: build succeeds; the final `cargo build` step compiles `rclrs` and `sensor_msgs` against Lyrical (an empty `ros` module is fine — the goal here is proving the shim + submodule + message-gen resolve and compile).
If the message-gen step produced nothing under `/opt/ros/lyrical/share/sensor_msgs/rust`, the `cargo build` will fail at the `sensor_msgs` patch — fix the ported colcon line before proceeding.

- [ ] **Step 7: Commit**

```bash
git add vantage-robot/Cargo.toml vantage-robot/src/main.rs vantage-robot/src/ros/mod.rs \
        .gitmodules third-party/ros2_rust ros-build/cargo-config.toml ros-build/Dockerfile
git commit -m "build(robot): ros feature, vendored rclrs submodule, ROS build toolchain"
```

---

## Task 3: ROS node, publishers, and feature-gated wiring

Fill `ros/mod.rs` with the node + publishers + publish mapping, and switch the raw-branch drain in `main.rs` to the ROS publisher under `--features ros`.

**Files:**
- Modify: `vantage-robot/src/ros/mod.rs` (replace placeholder)
- Modify: `vantage-robot/src/main.rs:37-44` (bridge construction) and `:58-72` (the drain)

**Interfaces:**
- Consumes: `crate::convert::{image_parts, camera_info_parts}` (Task 1); `vantage_signalling::peer::{Peer, RawFrame}`; `rclrs`, `sensor_msgs` (Task 2 feature).
- Produces:
  - `pub struct CameraBridge`
  - `pub fn CameraBridge::new() -> Result<CameraBridge, rclrs::RclrsError>`
  - `pub fn CameraBridge::publish(&self, frame: RawFrame) -> Result<(), rclrs::RclrsError>`
  - `pub fn CameraBridge::node(&self) -> std::sync::Arc<rclrs::Node>`

- [ ] **Step 1: Implement the ROS bridge**

Replace the entire contents of `vantage-robot/src/ros/mod.rs`:

```rust
//! Feature-gated ROS 2 camera bridge. The ONLY place `rclrs`/`sensor_msgs`
//! appear. Maps the pure parts from `crate::convert` onto ROS messages and
//! publishes `~/image_raw` + `~/camera_info`.

use std::sync::Arc;

use rclrs::{create_node, Context, Node, Publisher, RclrsError, QOS_PROFILE_DEFAULT};
use vantage_signalling::peer::RawFrame;

use crate::convert::{camera_info_parts, image_parts};

pub struct CameraBridge {
    node: Arc<Node>,
    image_pub: Arc<Publisher<sensor_msgs::msg::Image>>,
    info_pub: Arc<Publisher<sensor_msgs::msg::CameraInfo>>,
    frame_id: String,
}

impl CameraBridge {
    /// Create the ROS context, node, and the two publishers. The local `Context`
    /// may drop after this — the `Node` keeps it alive internally.
    pub fn new() -> Result<Self, RclrsError> {
        let context = Context::new(std::env::args())?;
        let node_name =
            std::env::var("VANTAGE_CAMERA_NODE").unwrap_or_else(|_| "vantage_camera".to_string());
        let node = create_node(&context, &node_name)?;
        // Relative (`~/`) topics so they remap/namespace. Default QoS; rmw-agnostic.
        let image_pub = node.create_publisher("~/image_raw", QOS_PROFILE_DEFAULT)?;
        let info_pub = node.create_publisher("~/camera_info", QOS_PROFILE_DEFAULT)?;
        let frame_id = std::env::var("VANTAGE_CAMERA_FRAME_ID")
            .unwrap_or_else(|_| "camera_optical_frame".to_string());
        Ok(Self { node, image_pub, info_pub, frame_id })
    }

    /// Publish one frame as Image + CameraInfo, sharing one timestamp + frame_id.
    pub fn publish(&self, frame: RawFrame) -> Result<(), RclrsError> {
        let ci = camera_info_parts(&frame); // borrow first...
        let parts = image_parts(frame); // ...then move the pixel buffer.
        let (sec, nanosec) = now_secs_nanos();

        // Build via Default + field mutation so we only ever name `sensor_msgs`
        // (header/stamp nested types come from Default — keeps deps to rclrs +
        // sensor_msgs exactly, per the spec's Cargo snippet).
        let mut img = sensor_msgs::msg::Image::default();
        img.header.stamp.sec = sec;
        img.header.stamp.nanosec = nanosec;
        img.header.frame_id = self.frame_id.clone();
        img.height = parts.height;
        img.width = parts.width;
        img.encoding = parts.encoding;
        img.is_bigendian = parts.is_bigendian;
        img.step = parts.step;
        img.data = parts.data;
        self.image_pub.publish(img)?;

        let mut info = sensor_msgs::msg::CameraInfo::default();
        info.header.stamp.sec = sec;
        info.header.stamp.nanosec = nanosec;
        info.header.frame_id = self.frame_id.clone();
        info.height = ci.height;
        info.width = ci.width;
        info.distortion_model = ci.distortion_model;
        self.info_pub.publish(info)?;
        Ok(())
    }

    /// The node handle, for `rclrs::spin` on a dedicated thread.
    pub fn node(&self) -> Arc<Node> {
        Arc::clone(&self.node)
    }
}

/// Wall-clock stamp as (sec, nanosec). Uses the system clock rather than the
/// rclrs node clock to avoid the `lyrical → rolling` clock-API drift risk;
/// sufficient for 4b (placeholder calibration). Swap for `node.get_clock()`
/// once the Lyrical clock API is confirmed.
fn now_secs_nanos() -> (i32, u32) {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    (d.as_secs() as i32, d.subsec_nanos())
}
```

- [ ] **Step 2: Verify the ROS module compiles in the container**

Run: `docker build -f ros-build/Dockerfile -t vantage-ros .`
Expected: build succeeds; the `cargo build --features ros` step now type-checks `sensor_msgs::msg::{Image, CameraInfo}` construction and links `rclrs` — proving the message-crate patch and the convert→message mapping are correct.

- [ ] **Step 3: Wire the bridge into `main.rs` (construct once at startup)**

In `vantage-robot/src/main.rs`, after `let mut sampler = Sampler::new();` (currently line 40), add the gated bridge construction. The bridge is created once and shared; `rclrs::spin` runs on a dedicated OS thread so the tokio runtime is never blocked:

```rust
    let mut sampler = Sampler::new();

    #[cfg(feature = "ros")]
    let ros_bridge = {
        let bridge = std::sync::Arc::new(ros::CameraBridge::new()?);
        let node = bridge.node();
        std::thread::spawn(move || {
            if let Err(e) = rclrs::spin(node) {
                tracing::warn!("rclrs spin ended: {e}");
            }
        });
        bridge
    };
```

(`CameraBridge::new()?` returns `rclrs::RclrsError`, which is `Error + Send + Sync + 'static`, so `?` converts cleanly into `anyhow::Result`.)

- [ ] **Step 4: Feature-gate the raw-branch drain**

Replace the existing drain block in the `ClientConnected` arm (`vantage-robot/src/main.rs:58-72`, the `{ let p_raw = p.clone(); tokio::spawn(...) }` block) with the two cfg variants. The `not(ros)` arm is the existing 4a behaviour, unchanged:

```rust
                        // Drain the raw (pre-encode) branch concurrently with the
                        // WebRTC stream. recv_raw_frame locks one receiver, so exactly
                        // one drain owns it — selected at compile time by the feature.
                        #[cfg(feature = "ros")]
                        {
                            let p_raw = p.clone();
                            let bridge = ros_bridge.clone();
                            tokio::spawn(async move {
                                let mut n: u64 = 0;
                                while let Some(frame) = p_raw.recv_raw_frame().await {
                                    n += 1;
                                    let (w, h) = (frame.width, frame.height);
                                    match bridge.publish(frame) {
                                        Ok(()) => {
                                            if n == 1 || n % 30 == 0 {
                                                tracing::info!("published ros image {w}x{h} (#{n})");
                                            }
                                        }
                                        Err(e) => tracing::warn!("ros publish failed: {e}"),
                                    }
                                }
                            });
                        }
                        #[cfg(not(feature = "ros"))]
                        {
                            let p_raw = p.clone();
                            tokio::spawn(async move {
                                let mut n: u64 = 0;
                                while let Some(frame) = p_raw.recv_raw_frame().await {
                                    n += 1;
                                    if n == 1 || n % 30 == 0 {
                                        tracing::info!(
                                            "raw frame {}x{} {} (#{n})",
                                            frame.width, frame.height, frame.encoding
                                        );
                                    }
                                }
                            });
                        }
```

- [ ] **Step 5: Verify both lanes build**

Run (default, in the dev shell): `cargo build -p vantage-robot && cargo test --workspace`
Expected: PASS — no ROS deps compiled; the `not(ros)` drain is used.
Run (ROS lane): `docker build -f ros-build/Dockerfile -t vantage-ros .`
Expected: build succeeds — the full publisher + wiring compile under `--features ros`.

- [ ] **Step 6: Commit**

```bash
git add vantage-robot/src/ros/mod.rs vantage-robot/src/main.rs
git commit -m "feat(robot): publish camera frames as sensor_msgs Image+CameraInfo (ros feature)"
```

---

## Task 4: Integration evidence + CI two lanes

Prove the exit criteria end to end in the container, capture the evidence, and encode both lanes in CI.

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `docs/superpowers/plans/notes/2026-06-21-phase4b-exit.md`

**Interfaces:**
- Consumes: the `vantage-ros` image (Task 2/3), the existing `vantage-coordinator` + `vantage-client`.

- [ ] **Step 1: Run the stack and confirm topics publish concurrently with the stream**

In the container (or native Lyrical env), source ROS and run coordinator + robot (`--features ros`) + a client to drive the WebRTC stream. Then, in a second sourced shell:

Run: `ros2 topic list`
Expected: includes `/vantage_camera/image_raw` and `/vantage_camera/camera_info`.

Run: `ros2 topic echo --once /vantage_camera/image_raw --field encoding`
Expected: `rgb8`.

Run: `ros2 topic echo --once /vantage_camera/image_raw --field width` and `--field height`
Expected: the live camera dimensions (matching the robot's log `published ros image WxH`).

Run: `ros2 topic echo --once /vantage_camera/camera_info --field distortion_model`
Expected: `plumb_bob`, with `width`/`height` matching the image.

While the above runs, confirm the **client still decodes video** (the WebRTC stream is unaffected) — the camera is shared, not monopolised. This mirrors the Phase 4a concurrency evidence.

- [ ] **Step 2: Record the exit evidence**

Create `docs/superpowers/plans/notes/2026-06-21-phase4b-exit.md` capturing the actual command output from Step 1 (topic list, the four `topic echo` results, the robot's `published ros image` log line, and a note that the client kept decoding). Model it on `docs/superpowers/plans/notes/2026-06-19-phase4a-exit.md`.

- [ ] **Step 3: Write CI — two lanes**

Create `.github/workflows/ci.yml`:

```yaml
name: ci
on: [push, pull_request]

jobs:
  default:
    name: default (no ROS)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install GStreamer (vantage-signalling)
        run: |
          sudo apt-get update
          sudo apt-get install -y --no-install-recommends \
            libgstreamer1.0-dev libgstreamer-plugins-base1.0-dev \
            gstreamer1.0-plugins-good gstreamer1.0-plugins-bad gstreamer1.0-libav
      - name: Test workspace (ROS-free)
        run: cargo test --workspace
      - name: Assert no ROS deps
        run: |
          ! cargo tree -p vantage-robot | grep -i '^rclrs\|sensor_msgs' \
            || (echo "default lane pulled in ROS deps" && exit 1)

  ros:
    name: ros (Lyrical container)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          submodules: recursive
      - name: Build ROS image (compiles --features ros against Lyrical)
        run: docker build -f ros-build/Dockerfile -t vantage-ros .
```

> The `ros` job's submodule is the `file://` local path from Task 2, which CI runners cannot reach. When the submodule is repointed to a published fork / PR #640, this job becomes fully reproducible. Until then it serves as the local reproducible recipe; note this in the commit so the limitation is explicit, not silent.

- [ ] **Step 4: Verify CI locally**

Run: `cargo test --workspace` (the `default` job's core)
Expected: PASS.
Run: `docker build -f ros-build/Dockerfile -t vantage-ros .` (the `ros` job)
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml docs/superpowers/plans/notes/2026-06-21-phase4b-exit.md
git commit -m "ci: two lanes (default ROS-free + Lyrical container) + phase4b exit evidence"
```

---

## Self-Review

**Spec coverage:**
- "Raw image availability" → `~/image_raw` `sensor_msgs/Image` (Tasks 1, 3; verified Task 4 Step 1). ✅
- "Camera info published" → `~/camera_info` `sensor_msgs/CameraInfo` (Tasks 1, 3; verified Task 4). ✅
- "Camera not monopolised" → feature-gated drain consumes the existing raw branch; client still decodes (Task 3, Task 4 Step 1). ✅
- "Optional compressed image" → explicitly deferred, no task (matches spec). ✅
- Module layout `main.rs`/`ros/mod.rs`/`convert.rs` → Tasks 1–3. ✅
- Pure ROS-free `convert.rs`, tested unconditionally → Task 1. ✅
- Cargo features (`ros = ["dep:rclrs","dep:sensor_msgs"]`, off by default) → Task 2. ✅
- Native Lyrical, `setup.zsh`/`setup.bash`, rmw-agnostic, no `~/ros2_ws` in build, submodule vendoring, ROS-only `[patch]` → Global Constraints + Task 2. ✅
- Two-lane CI + Lyrical Dockerfile → Tasks 2 (Dockerfile) + 4 (CI). ✅
- Exit criteria (workspace green; container publishes correct dims; concurrency; feature off by default) → Task 1 Step 5, Task 4 Step 1, Task 4 Step 1, Task 1 Step 5 / Task 3 Step 5. ✅

**Type consistency:** `ImageParts`/`CameraInfoParts` fields and `image_parts`(consumes)/`camera_info_parts`(borrows) signatures match between Task 1 (definition) and Task 3 (use). `CameraBridge::{new, publish, node}` signatures match between Task 3's Interfaces block and implementation, and the `main.rs` call sites use exactly those. ✅

**Known-uncertain points (surfaced, not hidden):**
1. Registry resolvability of `rclrs 0.7` / `sensor_msgs` for the default lane — Task 2 Step 2 has an explicit fallback.
2. The Lyrical Rust message-gen colcon line — Task 2 Step 5 ports it from the proven `~/ros2_ws` rather than guessing.
3. Submodule is a local `file://` path until a fork / PR #640 exists — noted in Global Constraints and Task 4 Step 3.

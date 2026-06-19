# Vantage Phase 4a — Real camera, tee, encoder factory Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the robot's fixed `videotestsrc` send path with a selectable camera source feeding a pre-encode `tee` — one branch encodes to WebRTC via a **runtime-selected** H.264 encoder (hardware where available, `x264enc` fallback), the other branch exposes raw RGB frames to Rust — so a live camera renders on the client while a concurrent raw branch is ready for the ROS2 bridge.

**Architecture:** In `vantage-signalling`, add an `encoder` module that probes the GStreamer registry for the best available H.264 encoder and configures it for low latency. Rewrite the robot's video branch to `source → videoconvert → caps → tee`, with the `tee` fanning out to (a) `queue(leaky) → encoder → h264parse → rtph264pay → webrtcbin` and (b) `queue → videoconvert → RGB appsink`, the latter emitting `RawFrame`s on a new `Peer::recv_raw_frame()` channel. A `VANTAGE_VIDEO_SOURCE` env var selects `test` (default, `videotestsrc`) or `camera` (`v4l2src`).

**Tech Stack:** Rust, GStreamer 1.28 via `gstreamer-rs` 0.23 (`gstreamer-app`, `gstreamer-video`), `webrtcbin`, `v4l2src`, `tee`, runtime H.264 encoder selection (`x264enc` confirmed; hardware encoders configured from docs).

**Scope:** This is **Phase 4a (media) only**, per the split decision. It delivers a live camera to the client and the encoder factory, and stands up the raw branch. The **ROS2 bridge** — publishing `sensor_msgs/Image` + `sensor_msgs/CameraInfo` from the raw branch via `rclrs`, with the colcon/ament build — is **Phase 4b** (separate plan; the `~/ros2_ws/src/ros2_rust` workspace + `cargo-ament-build` + `rclrs 0.7` are already present for it). Hardware decode, multi-consumer fan-out, demand-driven branches, keyframe-on-join, and adaptive bitrate remain **Phase 5**.

**Builds on:** Phase 3 (`docs/superpowers/plans/2026-06-18-vantage-phase3-video-testpattern.md`), merged to `main`. The robot's `Peer` (Role::Robot) currently builds `add_video_test_source` (videotestsrc → x264enc → rtph264pay → webrtcbin); the client decodes to a Slint window. This plan changes only the robot send side.

---

## Spec coverage (this plan)

- **video-streaming "One-way video over WebRTC"** under *real capture* — Tasks 2, 4 (camera frames reach the client).
- **Project non-negotiable** "hardware encode is selected at runtime with a software fallback" — Task 1 (the encoder factory).
- **camera-sharing** infrastructure: the pre-encode `tee` produces a concurrent raw branch (design.md §3 — tee sits before the encoder). The raw branch flowing *alongside* the WebRTC stream is the mechanism behind camera-sharing "Camera not monopolised by the stream" — Tasks 2, 3 prove the concurrent branch exists. Actually *publishing* raw `sensor_msgs/Image` + `camera_info` to the ROS2 graph (camera-sharing "Raw image availability", "Camera info published", "Optional compressed image") is **Phase 4b**.

## Prerequisites

All required elements confirmed present: `v4l2src`, `tee`, `queue`, `videoconvert`, `x264enc`, `h264parse`, `rtph264pay`, `appsink`. `/dev/video0` and `/dev/video1` exist. No hardware H.264 encoder is installed on this box, so the factory selects `x264enc` here — the hardware branches are configured from documented property names and would be exercised on a Jetson/GPU host.

> **Camera caveat:** a sandbox `/dev/video0` may not deliver frames or may be MJPEG-only. The `tee` + encoder factory + raw branch are fully verifiable with the default `test` source (Task 3). The live-`camera` path (Task 4) is verified against a working camera; where the sandbox camera doesn't produce frames, Task 4 records that the structure is proven via the test source and the camera path is confirmed on a camera host (mirroring how Phase 3 handled the windowed render).

---

## File Structure

```
vantage-signalling/
└── src/
    ├── encoder.rs   # NEW: runtime H.264 encoder selection + low-latency config
    ├── lib.rs       # + pub mod encoder;
    └── peer.rs      # RawFrame type, recv_raw_frame(); add_video_source (tee + factory)
                     #   replacing add_video_test_source
vantage-robot/
└── src/main.rs      # drain recv_raw_frame() (log concurrency); unchanged otherwise
```

Responsibility: encoder selection is isolated in `encoder.rs` (one concern, unit-testable). The pipeline graph stays in `peer.rs`. The robot just consumes the new raw channel.

---

### Task 1: Runtime H.264 encoder factory

**Files:**
- Create: `vantage-signalling/src/encoder.rs`
- Modify: `vantage-signalling/src/lib.rs`

- [ ] **Step 1: Write the encoder module with an inline test**

Create `vantage-signalling/src/encoder.rs`:

```rust
use anyhow::{Context, Result};
use gstreamer as gst;
use gstreamer::prelude::*;

/// Ordered H.264 encoder candidates: hardware first, software (`x264enc`) last.
/// design.md §8: nvv4l2h264enc (Jetson) → nvh264enc (desktop NVIDIA) →
/// vah264enc / vaapih264enc (Intel/AMD VAAPI) → qsvh264enc (Intel QSV) →
/// vtenc_h264 (macOS) → x264enc (software).
pub(crate) const CANDIDATES: &[&str] = &[
    "nvv4l2h264enc",
    "nvh264enc",
    "vah264enc",
    "vaapih264enc",
    "qsvh264enc",
    "vtenc_h264",
    "x264enc",
];

/// The factory name of the encoder that would be selected on this host, if any.
pub fn selected_encoder_name() -> Option<&'static str> {
    CANDIDATES
        .iter()
        .find(|n| gst::ElementFactory::find(n).is_some())
        .copied()
}

/// Build the first available H.264 encoder, configured for low latency (~1.5 Mbit/s,
/// ~1 s GOP at 30 fps, no B-frames). A downstream `h264parse` normalizes the output,
/// so the caps contract to `rtph264pay` is identical regardless of which is selected.
pub fn make_h264_encoder() -> Result<gst::Element> {
    let name = selected_encoder_name()
        .context("no H.264 encoder found (install gst-plugins-ugly for x264enc)")?;
    let enc = gst::ElementFactory::make(name)
        .build()
        .with_context(|| format!("failed to build encoder {name}"))?;
    configure_low_latency(&enc, name);
    tracing::info!("selected H.264 encoder: {name}");
    Ok(enc)
}

/// Set low-latency properties per encoder. Property names differ between encoders;
/// each arm sets only what that element understands. Bitrate UNITS differ (noted).
fn configure_low_latency(enc: &gst::Element, name: &str) {
    match name {
        "x264enc" => {
            enc.set_property_from_str("tune", "zerolatency");
            enc.set_property_from_str("speed-preset", "ultrafast");
            enc.set_property("bitrate", 1500u32); // kbit/s
            enc.set_property("key-int-max", 30u32);
        }
        "nvv4l2h264enc" => {
            // Jetson V4L2 encoder — bitrate in bits/s.
            enc.set_property("bitrate", 1_500_000u32);
            enc.set_property("iframeinterval", 30u32);
            enc.set_property("insert-sps-pps", true);
            enc.set_property("maxperf-enable", true);
        }
        "nvh264enc" => {
            // Desktop NVENC — bitrate in kbit/s.
            enc.set_property("bitrate", 1500u32);
            enc.set_property_from_str("preset", "low-latency-hp");
            enc.set_property("zerolatency", true);
        }
        "vah264enc" | "vaapih264enc" => {
            // VAAPI — bitrate in kbit/s.
            enc.set_property("bitrate", 1500u32);
            enc.set_property_from_str("rate-control", "cbr");
            enc.set_property("key-int-max", 30u32);
        }
        "qsvh264enc" => {
            // Intel QSV — bitrate in kbit/s.
            enc.set_property("bitrate", 1500u32);
            enc.set_property_from_str("rate-control", "cbr");
        }
        "vtenc_h264" => {
            // macOS VideoToolbox — bitrate in kbit/s.
            enc.set_property("bitrate", 1500u32);
            enc.set_property("realtime", true);
            enc.set_property("allow-frame-reordering", false);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factory_builds_an_available_encoder() {
        gst::init().unwrap();
        let enc = make_h264_encoder().expect("an H.264 encoder must be available");
        let factory_name = enc.factory().expect("element has a factory").name();
        assert!(
            CANDIDATES.contains(&factory_name.as_str()),
            "selected encoder {factory_name} not in the candidate list"
        );
    }

    #[test]
    fn selection_is_deterministic_and_present() {
        gst::init().unwrap();
        assert!(selected_encoder_name().is_some(), "expected at least x264enc");
    }
}
```

> **Binding caveat:** `set_property_from_str` is the post-build setter (the Phase 3 builder used `.property_from_str`). If a *hardware* encoder property name differs on a real Jetson/GPU host, fix it there — only `x264enc` is exercisable in this environment, and the test only asserts selection + build, which works for `x264enc`. Hardware property names are written from GStreamer docs; do not invent values for encoders you cannot test.

- [ ] **Step 2: Export the module**

In `vantage-signalling/src/lib.rs`, add:
```rust
pub mod encoder;
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p vantage-signalling encoder`
Expected: both tests pass (`factory_builds_an_available_encoder` selects `x264enc` here).

- [ ] **Step 4: Build the crate**

Run: `cargo build -p vantage-signalling`
Expected: 0 errors.

- [ ] **Step 5: Commit**

```bash
git add vantage-signalling/src/encoder.rs vantage-signalling/src/lib.rs
git commit -m "feat(signalling): runtime H.264 encoder factory (hw select, x264enc fallback)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Source selector + pre-encode tee + raw branch

Replace `add_video_test_source` with `add_video_source`: a selectable source into a `tee` that fans out to the encoder→WebRTC branch (using the Task 1 factory) and a raw RGB `appsink` branch that emits `RawFrame`s.

**Files:**
- Modify: `vantage-signalling/src/peer.rs`

- [ ] **Step 1: Add the `RawFrame` type and the raw-frames channel to `Peer`**

In `peer.rs`, after the `VideoFrame` struct, add:
```rust
/// One raw camera frame from the pre-encode tee branch (RGB888). Consumed by the
/// ROS2 bridge in Phase 4b; logged for concurrency verification in Phase 4a.
pub struct RawFrame {
    pub width: u32,
    pub height: u32,
    pub encoding: String, // "rgb8"
    pub data: Vec<u8>,    // tightly packed, width*height*3 bytes
}
```

Add two fields to the `Peer` struct (keep all existing fields):
```rust
    raw_frames_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<RawFrame>>,
    raw_frames_tx: mpsc::UnboundedSender<RawFrame>,
```

In `Peer::new`, where the frames channel is created, also create the raw channel and store it in the struct literal:
```rust
        let (raw_frames_tx, raw_frames_rx) = mpsc::unbounded_channel::<RawFrame>();
```
Add to the `Self { ... }` initializer:
```rust
            raw_frames_rx: tokio::sync::Mutex::new(raw_frames_rx),
            raw_frames_tx: raw_frames_tx.clone(),
```

- [ ] **Step 2: Add `recv_raw_frame` to `impl Peer`**

Next to `recv_frame`:
```rust
    /// Await the next raw camera frame (RGB888) from the pre-encode tee branch.
    pub async fn recv_raw_frame(&self) -> Option<RawFrame> {
        self.raw_frames_rx.lock().await.recv().await
    }
```

- [ ] **Step 3: Switch the robot arm to the new source builder**

In the `Role::Robot` arm of `Peer::new`, replace the call:
```rust
                add_video_test_source(&peer.pipeline, &peer.webrtcbin)?;
```
with:
```rust
                add_video_source(&peer.pipeline, &peer.webrtcbin, &peer.raw_frames_tx)?;
```

- [ ] **Step 4: Replace `add_video_test_source` with `add_video_source` + helpers**

Delete the existing `fn add_video_test_source(...)` and add these free functions at the bottom of `peer.rs` (next to `wire_video_receiver`):

```rust
/// Build the robot send graph: source → tee, with an H.264 encode branch into
/// webrtcbin (send-only) and a raw RGB branch into an appsink emitting RawFrames.
fn add_video_source(
    pipeline: &gst::Pipeline,
    webrtcbin: &gst::Element,
    raw_tx: &mpsc::UnboundedSender<RawFrame>,
) -> Result<()> {
    // --- source → videoconvert → caps(640x480@30) → tee ---
    let source = build_source()?;
    let srcconvert = gst::ElementFactory::make("videoconvert").build()?;
    let srccaps = gst::ElementFactory::make("capsfilter")
        .property("caps", &gst::Caps::builder("video/x-raw")
            .field("width", 640i32).field("height", 480i32)
            .field("framerate", gst::Fraction::new(30, 1))
            .build())
        .build()?;
    let tee = gst::ElementFactory::make("tee").name("t").build()?;

    pipeline.add_many([&source, &srcconvert, &srccaps, &tee])?;
    gst::Element::link_many([&source, &srcconvert, &srccaps, &tee])?;

    // --- encode branch: tee → queue(leaky) → encoder → h264parse → rtph264pay → webrtcbin ---
    let equeue = gst::ElementFactory::make("queue")
        .property_from_str("leaky", "downstream")
        .build()?;
    let enc = crate::encoder::make_h264_encoder()?;
    let parse = gst::ElementFactory::make("h264parse").build()?;
    let pay = gst::ElementFactory::make("rtph264pay")
        .property("config-interval", -1i32)
        .property("pt", 96u32)
        .build()?;
    let rtpcaps = gst::ElementFactory::make("capsfilter")
        .property("caps", &gst::Caps::builder("application/x-rtp")
            .field("media", "video").field("encoding-name", "H264").field("payload", 96i32)
            .build())
        .build()?;

    pipeline.add_many([&equeue, &enc, &parse, &pay, &rtpcaps])?;
    gst::Element::link_many([&equeue, &enc, &parse, &pay, &rtpcaps])?;
    link_tee(&tee, &equeue)?;

    let src_pad = rtpcaps.static_pad("src").context("rtpcaps has no src pad")?;
    let sink_pad = webrtcbin.request_pad_simple("sink_%u")
        .context("webrtcbin refused a sink pad")?;
    src_pad.link(&sink_pad)?;

    let transceiver = webrtcbin
        .emit_by_name::<gst_webrtc::WebRTCRTPTransceiver>("get-transceiver", &[&0i32]);
    transceiver.set_property("direction", gst_webrtc::WebRTCRTPTransceiverDirection::Sendonly);

    // --- raw branch: tee → queue → videoconvert → RGB appsink → RawFrame ---
    let rqueue = gst::ElementFactory::make("queue").build()?;
    let rconvert = gst::ElementFactory::make("videoconvert").build()?;
    let rawsink = gst_app::AppSink::builder()
        .caps(&gst::Caps::builder("video/x-raw").field("format", "RGB").build())
        .max_buffers(2)
        .drop(true)
        .build();
    {
        let raw_tx = raw_tx.clone();
        rawsink.set_callbacks(
            gst_app::AppSinkCallbacks::builder()
                .new_sample(move |sink| {
                    let sample = sink.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                    let buffer = sample.buffer().ok_or(gst::FlowError::Error)?;
                    let caps = sample.caps().ok_or(gst::FlowError::Error)?;
                    let info = gst_video::VideoInfo::from_caps(caps)
                        .map_err(|_| gst::FlowError::Error)?;
                    let frame = gst_video::VideoFrameRef::from_buffer_ref_readable(buffer, &info)
                        .map_err(|_| gst::FlowError::Error)?;
                    let width = info.width() as usize;
                    let height = info.height() as usize;
                    let stride = frame.plane_stride()[0] as usize;
                    let src = frame.plane_data(0).map_err(|_| gst::FlowError::Error)?;
                    let mut data = Vec::with_capacity(width * height * 3);
                    for row in 0..height {
                        let start = row * stride;
                        data.extend_from_slice(&src[start..start + width * 3]);
                    }
                    let _ = raw_tx.send(RawFrame {
                        width: width as u32,
                        height: height as u32,
                        encoding: "rgb8".into(),
                        data,
                    });
                    Ok(gst::FlowSuccess::Ok)
                })
                .build(),
        );
    }
    let rawsink_el: gst::Element = rawsink.upcast();
    pipeline.add_many([&rqueue, &rconvert, &rawsink_el])?;
    gst::Element::link_many([&rqueue, &rconvert, &rawsink_el])?;
    link_tee(&tee, &rqueue)?;

    // Elements added to an already-PLAYING pipeline must sync their state.
    for e in [
        &source, &srcconvert, &srccaps, &tee, &equeue, &enc, &parse, &pay, &rtpcaps,
        &rqueue, &rconvert, &rawsink_el,
    ] {
        e.sync_state_with_parent()?;
    }
    Ok(())
}

/// Request a `src_%u` pad off the tee and link it to a downstream element's sink.
fn link_tee(tee: &gst::Element, downstream: &gst::Element) -> Result<()> {
    let tee_src = tee.request_pad_simple("src_%u").context("tee has no src pad")?;
    let sink = downstream.static_pad("sink").context("downstream has no sink pad")?;
    tee_src.link(&sink)?;
    Ok(())
}

/// `VANTAGE_VIDEO_SOURCE=camera` → v4l2src (`VANTAGE_CAMERA_DEVICE`, default /dev/video0);
/// anything else (default) → videotestsrc SMPTE pattern.
fn build_source() -> Result<gst::Element> {
    let kind = std::env::var("VANTAGE_VIDEO_SOURCE").unwrap_or_else(|_| "test".into());
    if kind == "camera" {
        let dev = std::env::var("VANTAGE_CAMERA_DEVICE").unwrap_or_else(|_| "/dev/video0".into());
        tracing::info!("video source: camera {dev}");
        gst::ElementFactory::make("v4l2src")
            .property("device", dev.as_str())
            .build()
            .context("v4l2src — camera source")
    } else {
        tracing::info!("video source: test pattern");
        gst::ElementFactory::make("videotestsrc")
            .property_from_str("pattern", "smpte")
            .property("is-live", true)
            .build()
            .context("videotestsrc")
    }
}
```

> **MJPEG cameras:** some webcams only emit `image/jpeg`. `videoconvert` can't decode JPEG, so on such a camera `build_source` should be `v4l2src ! jpegdec`. Keep this in mind for Task 4; if the camera negotiation fails with a not-linked/caps error, insert a `jpegdec` after `v4l2src`. The default `test` source has no such issue.

- [ ] **Step 5: Verify the existing video test still passes**

Run: `cargo test -p vantage-signalling`
Expected: `peer_is_send_sync`, `robot_offer_contains_video_mline` (now built through the tee + factory encoder with the default test source), and the two `encoder` tests all pass. The `m=video` assertion confirms the rebuilt send graph still negotiates video.

- [ ] **Step 6: Build the workspace**

Run: `cargo build --workspace`
Expected: 0 errors. (`recv_raw_frame` is unused until Task 3 — that's a `dead_code` warning on the method, acceptable; do NOT add `#[allow]`, Task 3 consumes it.)

- [ ] **Step 7: Commit**

```bash
git add vantage-signalling/src/peer.rs
git commit -m "feat(signalling): pre-encode tee — encoder branch + raw RGB branch (RawFrame)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Robot drains the raw branch — prove concurrency (test source)

The robot logs raw-frame counts so we can confirm the raw branch runs **concurrently** with the WebRTC encode branch (the camera is not monopolised by the stream).

**Files:**
- Modify: `vantage-robot/src/main.rs`

- [ ] **Step 1: Spawn a raw-frame drain when the peer is built**

In `vantage-robot/src/main.rs`, the `ServerMsg::ClientConnected` arm builds `let p = Arc::new(Peer::new(&ice, Role::Robot)?);`. Immediately after assigning `peer = Some(p)` (or after creating `p`), spawn a task that drains `recv_raw_frame` and logs counts. Add (using a clone of the `Arc<Peer>`):

```rust
                    Some(ServerMsg::ClientConnected { session: s }) => {
                        tracing::info!("client connected: {s}");
                        let p = std::sync::Arc::new(Peer::new(&ice, Role::Robot)?);
                        // Drain the raw (pre-encode) branch so its channel doesn't grow,
                        // and log counts to prove it runs alongside the WebRTC stream.
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
                        peer = Some(p);
                        session = Some(s);
                        dc_open = false;
                    }
```

> Adapt to the actual local variable names in your `main.rs` (`peer`, `session`, `dc_open`, `ice`). The key change is: build the peer as an `Arc`, spawn the raw drain with a clone, then store it. If `peer` is already typed `Option<Arc<Peer>>` (it is, from Phase 2/3), this slots in directly.

- [ ] **Step 2: Build**

Run: `cargo build --workspace`
Expected: 0 errors (the `recv_raw_frame` dead_code warning from Task 2 is now gone).

- [ ] **Step 3: Headless end-to-end — concurrency with the test source**

```bash
cargo build -p vantage-coordinator -p vantage-robot -p vantage-client
export RUST_LOG=info
BIN=$(pwd)/target/debug
VANTAGE_BIND=127.0.0.1:8110 $BIN/vantage-coordinator >/tmp/p4_coord.log 2>&1 & CP=$!
for i in $(seq 1 40); do curl -sf http://127.0.0.1:8110/healthz >/dev/null 2>&1 && break; sleep 0.25; done
VANTAGE_COORDINATOR=ws://127.0.0.1:8110 $BIN/vantage-robot >/tmp/p4_robot.log 2>&1 & RP=$!
sleep 2
VANTAGE_HEADLESS=1 VANTAGE_COORDINATOR=ws://127.0.0.1:8110 $BIN/vantage-client >/tmp/p4_client.log 2>&1 & CLP=$!
for i in $(seq 1 16); do sleep 0.5; done
echo "client video frames: $(grep -c 'video frame' /tmp/p4_client.log)"
echo "robot raw frames:    $(grep -c 'raw frame' /tmp/p4_robot.log)"
echo "encoder selected:    $(grep -o 'selected H.264 encoder: .*' /tmp/p4_robot.log | head -1)"
grep -E 'raw frame' /tmp/p4_robot.log | tail -2
kill $CLP $RP $CP 2>/dev/null; wait 2>/dev/null
```

Expected: BOTH counts > 1 — the client decodes video frames AND the robot logs raw frames, simultaneously, from the same `tee`. The encoder log line shows `x264enc` on this host. This proves the camera/source is not monopolised by the stream (the raw branch runs concurrently).

If `robot raw frames` is 0, the raw branch isn't pulling — check `/tmp/p4_robot.log` for GStreamer link/caps errors and report BLOCKED with the log.

- [ ] **Step 4: Commit**

```bash
git add vantage-robot/src/main.rs
git commit -m "feat(robot): drain raw tee branch and log concurrency with the stream

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Live camera + integration

Exercise the real-camera path and confirm the exit criteria.

**Files:** none (verification only; optional evidence note).

- [ ] **Step 1: Run with the camera source**

```bash
export RUST_LOG=info
BIN=$(pwd)/target/debug
VANTAGE_BIND=127.0.0.1:8111 $BIN/vantage-coordinator >/tmp/p4c_coord.log 2>&1 & CP=$!
for i in $(seq 1 40); do curl -sf http://127.0.0.1:8111/healthz >/dev/null 2>&1 && break; sleep 0.25; done
VANTAGE_VIDEO_SOURCE=camera VANTAGE_CAMERA_DEVICE=/dev/video0 \
  VANTAGE_COORDINATOR=ws://127.0.0.1:8111 $BIN/vantage-robot >/tmp/p4c_robot.log 2>&1 & RP=$!
sleep 2
VANTAGE_HEADLESS=1 VANTAGE_COORDINATOR=ws://127.0.0.1:8111 $BIN/vantage-client >/tmp/p4c_client.log 2>&1 & CLP=$!
for i in $(seq 1 20); do sleep 0.5; done
echo "video source line:   $(grep -o 'video source: .*' /tmp/p4c_robot.log | head -1)"
echo "client video frames: $(grep -c 'video frame' /tmp/p4c_client.log)"
echo "robot raw frames:    $(grep -c 'raw frame' /tmp/p4c_robot.log)"
echo "robot errors:";        grep -iE 'error|not-negotiated|not-linked|cannot' /tmp/p4c_robot.log | head -3
kill $CLP $RP $CP 2>/dev/null; wait 2>/dev/null
```

Expected (working camera): `video source: camera /dev/video0`, client video frames > 1, robot raw frames > 1 — i.e. live camera renders on the client AND the raw branch produces frames simultaneously.

- [ ] **Step 2: Handle a non-delivering / MJPEG sandbox camera**

If the camera produces no frames or logs a caps/negotiation error (`not-negotiated`, `not-linked`, `Internal data stream error`):
- If it's MJPEG-only, edit `build_source` to insert a `jpegdec` (`v4l2src ! jpegdec`) — make a `v4l2src` element, a `jpegdec` element, wrap them in a `gst::Bin` with a ghost pad, or return a small bin; the simplest is to change `add_video_source` to special-case camera by adding `jpegdec` between source and `srcconvert`. Re-run.
- If the sandbox camera simply doesn't deliver frames, that is an environment limitation, not a code defect. Record that the `tee` + encoder factory + raw branch are proven via the `test` source (Task 3), and the `camera` path is to be confirmed on a host with a working camera — mirroring how Phase 3 recorded the windowed-render caveat.

- [ ] **Step 3: Windowed sanity (optional, needs a display)**

On a machine with a display, run the windowed client (no `VANTAGE_HEADLESS`) against the camera robot and confirm the live camera image renders in the Slint window beside telemetry.

- [ ] **Step 4: Record evidence**

Create `docs/superpowers/plans/notes/2026-06-19-phase4a-exit.md` capturing: the selected encoder, the concurrent video/raw counts (test source), and the camera-path result (working, or proven-via-test-source with the camera caveat).

```bash
git add docs/superpowers/plans/notes/2026-06-19-phase4a-exit.md
git commit -m "test: phase 4a camera/tee/encoder-factory exit evidence"
```

---

## Phase 4a exit criteria (gate before Phase 4b)

- [ ] `cargo test --workspace` green, including the two `encoder` tests and `robot_offer_contains_video_mline` (now built through the tee + factory).
- [ ] Headless (test source): client decodes video frames AND the robot logs raw frames simultaneously from one `tee` — concurrent raw + encode branches.
- [ ] Encoder factory selects a real encoder at runtime (`x264enc` here; hardware on a GPU/Jetson host) and the stream still decodes on the client.
- [ ] Camera path (`VANTAGE_VIDEO_SOURCE=camera`) renders a live camera on the client where a working camera exists (else proven via the test source with the camera caveat recorded).

Once green, **Phase 4b** consumes `Peer::recv_raw_frame()` to publish `sensor_msgs/Image` + `sensor_msgs/CameraInfo` over `rclrs` (camera-sharing "Raw image availability", "Camera info published", optional `CompressedImage`), introducing the colcon/ament build for the ROS-linked crate.

---

## Self-review

**Spec coverage:** the encoder factory (project non-negotiable, runtime hw select + sw fallback) → Task 1; real-capture one-way video → Tasks 2/4; the pre-encode `tee` giving a concurrent raw branch (the mechanism behind camera-sharing "Camera not monopolised") → Tasks 2/3. Publishing raw `Image`/`camera_info`/`CompressedImage` to the ROS2 graph is explicitly deferred to Phase 4b and called out — no in-scope-for-4a requirement is left without a task.

**Placeholder scan:** every code step is complete; verification steps give exact commands and expected output. The MJPEG/non-delivering-camera and hardware-encoder-property notes point at concrete resolutions rather than leaving anything as TODO. Hardware encoder property values are written from GStreamer docs and explicitly flagged as untested-here (honest, not a placeholder).

**Type consistency:** `RawFrame { width: u32, height: u32, encoding: String, data: Vec<u8> }`, `Peer::recv_raw_frame(&self) -> Option<RawFrame>`, `encoder::{make_h264_encoder() -> Result<gst::Element>, selected_encoder_name() -> Option<&'static str>, CANDIDATES}`, `add_video_source(&Pipeline, &Element, &UnboundedSender<RawFrame>)`, `link_tee`, `build_source` are used consistently across Tasks 1–3. The robot consumes `recv_raw_frame` (Task 3) exactly as Task 2 defines it. `add_video_test_source` is fully removed and replaced by `add_video_source` (no dangling reference).

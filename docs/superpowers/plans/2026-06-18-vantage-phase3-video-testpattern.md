# Vantage Phase 3 — Video (test pattern) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stream a software-encoded H.264 **test pattern** from the robot to the client over the existing WebRTC peer connection, decode it on the client, and render it in a Slint window **beside the live telemetry** already flowing over the data channel.

**Architecture:** Extend the `vantage-signalling::Peer` (today: data-channel only) so the robot (offerer) adds a `videotestsrc ! x264enc ! rtph264pay ! webrtcbin` **send-only** video branch, and the client (answerer) handles the incoming track with `webrtcbin pad-added → rtph264depay ! h264parse ! avdec_h264 ! videoconvert(RGBA) ! appsink`, emitting decoded `VideoFrame`s on a dedicated channel. The client binary is restructured to run the Slint event loop on the main thread while the WebRTC/telemetry session runs on a background Tokio thread; frames and telemetry are pushed into the UI via Slint's `upgrade_in_event_loop`.

**Tech Stack:** Rust, GStreamer 1.28 via `gstreamer-rs` 0.23 (`gstreamer-app`, `gstreamer-video`), `webrtcbin`, `x264enc`/`avdec_h264` (software, no GPU), Slint 1.x.

**Scope:** This is **Phase 3 only** — a *test pattern*, not the real camera. It deliberately isolates the two hard parts (webrtcbin video negotiation, and the decode→texture path — the design doc's main client risk) from camera/driver variables. Real `v4l2src`/`nvarguscamerasrc`, the `tee`, raw `sensor_msgs/Image` + `camera_info`, and the encoder factory are **Phase 4** (separate plan). Hardware decode, multi-consumer fan-out, demand-driven branches, keyframe-on-join, and adaptive bitrate are **Phase 5**.

**Builds on:** The foundation milestone (`docs/superpowers/plans/2026-06-17-vantage-poc-foundation.md`), merged to `main`. Signalling/relay are proven over both direct and TURN-relayed connections.

---

## Spec coverage (this plan)

From `openspec/changes/add-vantage-poc/specs/video-streaming/spec.md`:
- **One-way video over WebRTC** (`sendonly` transceiver, no client→robot video) — Tasks 1, 2, 4.

Deferred to later phases (explicitly out of scope here): "Encode once, fan out", "Immediate startup for new viewers", "Demand-driven streaming", "Adaptive bitrate" (all Phase 5); real camera + `camera-sharing` (Phase 4).

## Prerequisites

All required GStreamer elements are already present (verified): `videotestsrc`, `x264enc`, `rtph264pay`/`rtph264depay`, `h264parse`, `avdec_h264` (gst-libav), `videoconvert`, `appsink`. No new system package for the media path.

**Slint runtime needs a display** (X11 or Wayland). In a headless environment the windowed run (Task 5) cannot be verified; Task 3 adds a `VANTAGE_HEADLESS=1` mode that runs the full video pipeline and logs received frames **without** Slint, so the robot→client video path is verifiable without a display.

---

## File Structure

```
vantage-signalling/
├── Cargo.toml              # + gstreamer-app, gstreamer-video
└── src/
    └── peer.rs             # + Role enum, VideoFrame, video send branch (robot),
                            #   video recv→decode→appsink branch (client), recv_frame()
vantage-robot/
└── src/main.rs             # Peer::new(&ice, Role::Robot)
vantage-client/
├── Cargo.toml              # + slint
└── src/
    ├── main.rs             # Slint event loop on main thread; spawns session thread
    ├── session.rs          # run_session(): connect/discover/select-loop, drives UI sink
    └── ui.rs               # slint::slint!{} component (video Image + telemetry panel)
```

Responsibility split: all GStreamer/WebRTC stays in `peer.rs`; the client's UI (`ui.rs`), session/glue (`session.rs`), and process wiring (`main.rs`) are separated so the Slint thread-model concerns don't tangle with the WebRTC session logic.

---

### Task 1: Robot — H.264 test-pattern send branch + `Role` API

Replace the `create_data_channel: bool` argument with a `Role` enum and, for `Role::Robot`, add a send-only `videotestsrc → x264enc → webrtcbin` branch so the generated SDP offer carries a video m-line.

**Files:**
- Modify: `vantage-signalling/Cargo.toml`
- Modify: `vantage-signalling/src/peer.rs`
- Modify: `vantage-robot/src/main.rs`

- [ ] **Step 1: Add the gstreamer-app / gstreamer-video deps**

In `vantage-signalling/Cargo.toml`, under `[dependencies]`, add (matching the existing `v1_22` feature on the other gstreamer crates):

```toml
gstreamer-app = { version = "0.23", features = ["v1_22"] }
gstreamer-video = { version = "0.23", features = ["v1_22"] }
```

- [ ] **Step 2: Add the `Role` enum and `VideoFrame` type, and route construction by role**

In `vantage-signalling/src/peer.rs`, near the top (after the existing `use` lines), add:

```rust
use gstreamer_app as gst_app;
use gstreamer_video as gst_video;

/// Which end of the connection this peer is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Offerer: creates the telemetry data channel AND a send-only video branch.
    Robot,
    /// Answerer: receives the data channel and the incoming video track.
    Client,
}

/// One decoded RGBA video frame handed to the UI.
pub struct VideoFrame {
    pub width: u32,
    pub height: u32,
    pub rgba: Vec<u8>, // tightly packed, width*height*4 bytes
}
```

- [ ] **Step 3: Extend the `Peer` struct with a frames channel**

In the `Peer` struct definition, add two fields (keep the existing ones):

```rust
pub struct Peer {
    pub pipeline: gst::Pipeline,
    pub webrtcbin: gst::Element,
    events_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<PeerEvent>>,
    events_tx: mpsc::UnboundedSender<PeerEvent>,
    data_channel: std::sync::Mutex<Option<gst_webrtc::WebRTCDataChannel>>,
    frames_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<VideoFrame>>,
    frames_tx: mpsc::UnboundedSender<VideoFrame>,
}
```

- [ ] **Step 4: Change `Peer::new` to take `Role` and build the video send branch for the robot**

Change the signature and body. The new signature:

```rust
pub fn new(ice_servers: &[IceServer], role: Role) -> Result<Self> {
```

Inside, after the `webrtcbin` element is built and ICE servers are configured and `pipeline.add(&webrtcbin)?` has run, and after the `(tx, rx)` event channel is created, add the frames channel and branch by role. Replace the existing `if create_data_channel { ... } else { ... }` block with:

```rust
        let (frames_tx, frames_rx) = mpsc::unbounded_channel::<VideoFrame>();

        let peer = Self {
            pipeline,
            webrtcbin,
            events_rx: tokio::sync::Mutex::new(rx),
            events_tx: tx.clone(),
            data_channel: std::sync::Mutex::new(None),
            frames_rx: tokio::sync::Mutex::new(frames_rx),
            frames_tx: frames_tx.clone(),
        };

        match role {
            Role::Robot => {
                // Telemetry data channel (same as Phase 2).
                let dc = peer.webrtcbin.emit_by_name_with_values(
                    "create-data-channel",
                    &["telemetry".to_value(), None::<gst::Structure>.to_value()],
                );
                let dc = dc.get::<gst_webrtc::WebRTCDataChannel>()
                    .context("create-data-channel returned null (pipeline not ready?)")?;
                wire_data_channel(&dc, &tx);
                *peer.data_channel.lock().unwrap() = Some(dc);

                // Send-only H.264 test-pattern branch.
                add_video_test_source(&peer.pipeline, &peer.webrtcbin)?;

                // Offer once negotiation is needed.
                wire_on_negotiation_needed(&peer.webrtcbin, &tx);
            }
            Role::Client => {
                // Receive the remote-created data channel.
                {
                    let tx2 = tx.clone();
                    peer.webrtcbin.connect("on-data-channel", false, move |vals| {
                        let dc = vals[1].get::<gst_webrtc::WebRTCDataChannel>().unwrap();
                        wire_data_channel(&dc, &tx2);
                        None
                    });
                }
                // Decode the incoming video track into VideoFrames.
                wire_video_receiver(&peer.pipeline, &peer.webrtcbin, &frames_tx);
            }
        }

        peer.pipeline.set_state(gst::State::Playing)?;
        Ok(peer)
```

> If your current `peer.rs` inlines the `on-negotiation-needed` wiring rather than calling a `wire_on_negotiation_needed` helper, extract it into the free function shown in Step 5 so both this match arm and the helper agree. Keep the exact offer/answer logic you already have — only move it.

- [ ] **Step 5: Add the helper functions**

Add these free functions at the bottom of `peer.rs` (alongside the existing `wire_data_channel`/`parse_sdp`). `wire_on_negotiation_needed` is your existing offer logic, extracted verbatim into a function:

```rust
fn wire_on_negotiation_needed(webrtcbin: &gst::Element, tx: &mpsc::UnboundedSender<PeerEvent>) {
    let bin = webrtcbin.clone();
    let txn = tx.clone();
    webrtcbin.connect("on-negotiation-needed", false, move |_| {
        let bin2 = bin.clone();
        let tx2 = txn.clone();
        let promise = gst::Promise::with_change_func(move |reply| {
            let reply = match reply { Ok(Some(r)) => r, _ => return };
            let offer = reply.value("offer").unwrap()
                .get::<gst_webrtc::WebRTCSessionDescription>().unwrap();
            bin2.emit_by_name::<()>("set-local-description", &[&offer, &None::<gst::Promise>]);
            let sdp = offer.sdp().as_text().unwrap();
            let _ = tx2.send(PeerEvent::LocalDescription(Signal::Offer { sdp }));
        });
        bin.emit_by_name::<()>("create-offer", &[&None::<gst::Structure>, &promise]);
        None
    });
}

/// videotestsrc → x264enc(H.264) → rtph264pay → webrtcbin, send-only.
fn add_video_test_source(pipeline: &gst::Pipeline, webrtcbin: &gst::Element) -> Result<()> {
    let src = gst::ElementFactory::make("videotestsrc")
        .property_from_str("pattern", "smpte")
        .property("is-live", true)
        .build()?;
    let caps = gst::ElementFactory::make("capsfilter")
        .property("caps", &gst::Caps::builder("video/x-raw")
            .field("width", 640i32).field("height", 480i32)
            .field("framerate", gst::Fraction::new(30, 1))
            .build())
        .build()?;
    let convert = gst::ElementFactory::make("videoconvert").build()?;
    let enc = gst::ElementFactory::make("x264enc")
        .property_from_str("tune", "zerolatency")
        .property_from_str("speed-preset", "ultrafast")
        .property("bitrate", 1500u32)
        .property("key-int-max", 30u32)
        .build()?;
    let pay = gst::ElementFactory::make("rtph264pay")
        .property("config-interval", -1i32)
        .property("pt", 96u32)
        .build()?;
    let rtpcaps = gst::ElementFactory::make("capsfilter")
        .property("caps", &gst::Caps::builder("application/x-rtp")
            .field("media", "video").field("encoding-name", "H264").field("payload", 96i32)
            .build())
        .build()?;

    pipeline.add_many([&src, &caps, &convert, &enc, &pay, &rtpcaps])?;
    gst::Element::link_many([&src, &caps, &convert, &enc, &pay, &rtpcaps])?;

    // Link the RTP caps source pad into a webrtcbin sink request pad.
    let src_pad = rtpcaps.static_pad("src").context("rtpcaps has no src pad")?;
    let sink_pad = webrtcbin.request_pad_simple("sink_%u")
        .context("webrtcbin refused a sink pad")?;
    src_pad.link(&sink_pad)?;

    // Make the transceiver send-only (no client→robot video). Best-effort: if the
    // transceiver isn't retrievable, sendrecv still yields one-way flow because the
    // client answers recvonly (it has no video source).
    let transceiver = webrtcbin
        .emit_by_name::<gst_webrtc::WebRTCRTPTransceiver>("get-transceiver", &[&0i32]);
    transceiver.set_property("direction", gst_webrtc::WebRTCRTPTransceiverDirection::Sendonly);

    Ok(())
}
```

> **Binding caveat (same as Phase 2 Task 9):** exact `gstreamer-rs` 0.23 spellings (`property_from_str`, `request_pad_simple`, `get-transceiver` return type, `WebRTCRTPTransceiverDirection`) may need small adjustments. The success criterion is a clean compile and the m=video assertion in Step 7. If a call resists, read the installed crate source under `~/.cargo/registry/src/*/gstreamer-*-0.23*/` (Context7 MCP may be denied).

- [ ] **Step 6: Update existing call sites and the existing construct test**

In `vantage-robot/src/main.rs`, change `Peer::new(&ice, true)?` to `Peer::new(&ice, Role::Robot)?` and add `Role` to the import:
```rust
use vantage_signalling::peer::{Peer, PeerEvent, Role};
```

In `vantage-signalling/src/peer.rs`, the existing `#[cfg(test)] mod tests` currently calls `Peer::new(&ice, true)`. Update it and the `Send+Sync` assertion to use the new API (replace the whole test module body):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn ice() -> Vec<IceServer> {
        vec![IceServer {
            urls: vec!["stun:stun.l.google.com:19302".into()],
            username: None,
            credential: None,
        }]
    }

    #[test]
    fn peer_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Peer>();
    }

    #[tokio::test]
    async fn robot_offer_contains_video_mline() {
        let peer = Peer::new(&ice(), Role::Robot).expect("robot peer builds");
        // on-negotiation-needed fires after PLAYING; the offer is emitted as an event.
        let ev = tokio::time::timeout(std::time::Duration::from_secs(5), peer.recv_event())
            .await
            .expect("offer produced within 5s");
        match ev {
            Some(PeerEvent::LocalDescription(Signal::Offer { sdp })) => {
                assert!(sdp.contains("m=video"), "offer must contain a video m-line:\n{sdp}");
            }
            other => panic!("expected an Offer, got {other:?}"),
        }
    }
}
```

(`PeerEvent` must derive `Debug` for the `panic!("{other:?}")`. If it doesn't already, add `#[derive(Debug)]` above `pub enum PeerEvent` — `Vec<u8>` and `Signal` are `Debug`, so this compiles.)

- [ ] **Step 7: Run the test to verify the video m-line appears**

Run: `cargo test -p vantage-signalling robot_offer_contains_video_mline -- --nocapture`
Expected: PASS. (This exercises `gst::init`, the full `videotestsrc→x264enc→webrtcbin` link, transceiver direction, and local SDP offer generation — no network needed.)

- [ ] **Step 8: Confirm the workspace still builds and the robot is unchanged behaviorally**

Run: `cargo build --workspace`
Expected: 0 errors. (`vantage-client` still calls the old `Peer::new(&ice, false)` — it will FAIL to compile until Task 2 updates it. If so, temporarily change the client call to `Peer::new(&ice, Role::Client)` now and leave the rest of Task 2 for the next task, OR build only the changed crates: `cargo build -p vantage-signalling -p vantage-robot`.) Prefer building just those two here; Task 2 finishes the client.

- [ ] **Step 9: Commit**

```bash
git add vantage-signalling/Cargo.toml vantage-signalling/src/peer.rs vantage-robot/src/main.rs Cargo.lock
git commit -m "feat(signalling): robot send-only H.264 test-pattern video branch + Role API

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Client — receive, decode, and emit `VideoFrame`s

Add the `webrtcbin pad-added → depay → decode → RGBA appsink` branch and expose decoded frames via `recv_frame`. Update the client's `Peer::new` call.

**Files:**
- Modify: `vantage-signalling/src/peer.rs`
- Modify: `vantage-client/src/main.rs` (the `Peer::new` call only; full restructure is Task 4)

- [ ] **Step 1: Add `recv_frame` to the `Peer` impl**

In `impl Peer`, alongside `recv_event`, add:

```rust
    /// Await the next decoded video frame (RGBA). Returns None when the pipeline ends.
    pub async fn recv_frame(&self) -> Option<VideoFrame> {
        self.frames_rx.lock().await.recv().await
    }
```

- [ ] **Step 2: Add the video receiver wiring**

Add this free function at the bottom of `peer.rs`:

```rust
/// On the answerer, decode the incoming H.264 track to RGBA frames.
fn wire_video_receiver(
    pipeline: &gst::Pipeline,
    webrtcbin: &gst::Element,
    frames_tx: &mpsc::UnboundedSender<VideoFrame>,
) {
    let pipeline = pipeline.clone();
    let frames_tx = frames_tx.clone();
    webrtcbin.connect_pad_added(move |_bin, pad| {
        // webrtcbin creates a recv src pad carrying application/x-rtp when media arrives.
        if pad.direction() != gst::PadDirection::Src {
            return;
        }
        if let Err(e) = build_decode_branch(&pipeline, pad, &frames_tx) {
            tracing::error!("failed to build decode branch: {e}");
        }
    });
}

fn build_decode_branch(
    pipeline: &gst::Pipeline,
    src_pad: &gst::Pad,
    frames_tx: &mpsc::UnboundedSender<VideoFrame>,
) -> Result<()> {
    let depay = gst::ElementFactory::make("rtph264depay").build()?;
    let parse = gst::ElementFactory::make("h264parse").build()?;
    let dec = gst::ElementFactory::make("avdec_h264").build()?;
    let convert = gst::ElementFactory::make("videoconvert").build()?;

    let appsink = gst_app::AppSink::builder()
        .caps(&gst::Caps::builder("video/x-raw").field("format", "RGBA").build())
        .max_buffers(2)
        .drop(true) // drop late frames rather than build latency
        .build();

    {
        let frames_tx = frames_tx.clone();
        appsink.set_callbacks(
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

                    // Copy row-by-row to drop any stride padding -> tightly packed RGBA.
                    let mut rgba = Vec::with_capacity(width * height * 4);
                    for row in 0..height {
                        let start = row * stride;
                        rgba.extend_from_slice(&src[start..start + width * 4]);
                    }
                    let _ = frames_tx.send(VideoFrame {
                        width: width as u32,
                        height: height as u32,
                        rgba,
                    });
                    Ok(gst::FlowSuccess::Ok)
                })
                .build(),
        );
    }

    let sink_el: gst::Element = appsink.upcast();
    let elements = [&depay, &parse, &dec, &convert, &sink_el];
    pipeline.add_many(elements)?;
    gst::Element::link_many(elements)?;
    for e in elements {
        e.sync_state_with_parent()?;
    }

    let depay_sink = depay.static_pad("sink").context("depay has no sink pad")?;
    src_pad.link(&depay_sink)?;
    Ok(())
}
```

> **Binding caveats:** `connect_pad_added`, `AppSink::builder()`, `AppSinkCallbacks`, `VideoFrameRef::from_buffer_ref_readable`, and `plane_stride()`/`plane_data()` are the 0.23 spellings; adjust if the compiler disagrees (read the installed `gstreamer-app`/`gstreamer-video` sources). The decode element is `avdec_h264` (gst-libav, confirmed installed). If a deployment lacks libav, swap to `decodebin` (auto-plug) — but that adds a second dynamic pad to handle, so prefer `avdec_h264` here.

- [ ] **Step 3: Update the client's `Peer::new` call (compile-only; full client restructure is Task 4)**

In `vantage-client/src/main.rs`, change `Peer::new(&ice, false)?` to `Peer::new(&ice, Role::Client)?`, and add `Role` to the import from `vantage_signalling::peer`. (The rest of the client is rewritten in Task 4; this keeps the workspace compiling now.)

- [ ] **Step 4: Build the workspace**

Run: `cargo build --workspace`
Expected: 0 errors.

- [ ] **Step 5: Run the signalling tests**

Run: `cargo test -p vantage-signalling`
Expected: `peer_is_send_sync` and `robot_offer_contains_video_mline` pass. (`Peer` still `Send + Sync` after adding the frames channel.)

- [ ] **Step 6: Commit**

```bash
git add vantage-signalling/src/peer.rs vantage-client/src/main.rs
git commit -m "feat(signalling): client video receive->decode->RGBA frames (appsink)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Headless end-to-end — prove frames flow robot→client

Restructure the client into a reusable `run_session` and add a `VANTAGE_HEADLESS=1` mode that runs the full pipeline without Slint and logs received frames. This is the automated verification for the video path (no display needed).

**Files:**
- Create: `vantage-client/src/session.rs`
- Modify: `vantage-client/src/main.rs`

- [ ] **Step 1: Define a UI sink trait and the session in `session.rs`**

Create `vantage-client/src/session.rs`. `run_session` takes a `FrameSink` so the same session drives either the headless logger (this task) or the Slint UI (Task 4):

```rust
use anyhow::Result;
use vantage_protocol::codec;
use vantage_protocol::signalling::{ClientMsg, IceServer, ServerMsg};
use vantage_protocol::telemetry::DeviceInfo;
use vantage_signalling::peer::{Peer, PeerEvent, Role, VideoFrame};
use vantage_signalling::ws::CoordinatorWs;

use std::sync::Arc;

/// Where the session pushes decoded frames, telemetry, and status. Implementations
/// are the headless logger (Task 3) and the Slint UI bridge (Task 4).
pub trait UiSink: Send + Sync + 'static {
    fn frame(&self, frame: VideoFrame);
    fn telemetry(&self, info: &DeviceInfo);
    fn status(&self, text: &str);
}

pub async fn run_session(coord: String, ui: Arc<dyn UiSink>) -> Result<()> {
    let mut ws = CoordinatorWs::connect(&format!("{coord}/ws/client")).await?;

    ws.send(&ClientMsg::ListRobots).await?;
    let robots = loop {
        match ws.recv::<ServerMsg>().await? {
            Some(ServerMsg::RobotList { robots }) => break robots,
            Some(_) => continue,
            None => anyhow::bail!("coordinator closed before sending robot list"),
        }
    };
    let target = robots.into_iter().next().ok_or_else(|| anyhow::anyhow!("no robots online"))?;
    ui.status(&format!("connecting to {}", target.name));

    ws.send(&ClientMsg::Connect { robot: target.id.clone() }).await?;

    let ice = fetch_ice(&coord).await?;
    let peer = Arc::new(Peer::new(&ice, Role::Client)?);

    // Frame pump: decoded frames -> UI.
    {
        let peer = peer.clone();
        let ui = ui.clone();
        tokio::spawn(async move {
            while let Some(frame) = peer.recv_frame().await {
                ui.frame(frame);
            }
        });
    }

    let (mut tx, mut rx) = ws.split();
    loop {
        tokio::select! {
            msg = rx.recv::<ServerMsg>() => {
                match msg? {
                    Some(ServerMsg::Signal { signal, .. }) => { peer.handle_signal(signal)?; }
                    Some(ServerMsg::Error { message }) => ui.status(&format!("error: {message}")),
                    Some(_) => {}
                    None => { ui.status("coordinator closed"); break; }
                }
            }
            ev = peer.recv_event() => {
                match ev {
                    Some(PeerEvent::LocalDescription(sig)) | Some(PeerEvent::LocalIce(sig)) => {
                        tx.send(&ClientMsg::Signal { signal: sig }).await?;
                    }
                    Some(PeerEvent::DataChannelOpen) => ui.status("connected"),
                    Some(PeerEvent::DataMessage(bytes)) => {
                        if let Ok(info) = codec::decode::<DeviceInfo>(&bytes) {
                            ui.telemetry(&info);
                        }
                    }
                    None => {}
                }
            }
        }
    }
    Ok(())
}

async fn fetch_ice(coord: &str) -> Result<Vec<IceServer>> {
    let http = coord.replacen("ws", "http", 1);
    let servers = reqwest::get(format!("{http}/ice")).await?.json::<Vec<IceServer>>().await?;
    Ok(servers)
}
```

- [ ] **Step 2: Headless `main` that uses a logging `UiSink`**

Replace `vantage-client/src/main.rs` with a headless-capable entry point (Slint wiring is added in Task 4):

```rust
mod session;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use session::{run_session, UiSink};
use vantage_protocol::telemetry::DeviceInfo;
use vantage_signalling::peer::VideoFrame;

struct LogSink {
    frames: AtomicU64,
}
impl UiSink for LogSink {
    fn frame(&self, frame: VideoFrame) {
        let n = self.frames.fetch_add(1, Ordering::Relaxed) + 1;
        if n == 1 || n % 30 == 0 {
            tracing::info!("video frame {}x{} (#{n})", frame.width, frame.height);
        }
    }
    fn telemetry(&self, info: &DeviceInfo) {
        tracing::info!("telemetry: cpu={:.1}% mem={}/{}MB", info.cpu_percent, info.mem_used_mb, info.mem_total_mb);
    }
    fn status(&self, text: &str) { tracing::info!("status: {text}"); }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let coord = std::env::var("VANTAGE_COORDINATOR").unwrap_or_else(|_| "ws://localhost:8080".into());

    // Task 4 makes the default path launch Slint; for now everything is headless.
    let sink = Arc::new(LogSink { frames: AtomicU64::new(0) });
    run_session(coord, sink).await
}
```

- [ ] **Step 3: Build**

Run: `cargo build --workspace`
Expected: 0 errors.

- [ ] **Step 4: Run the full headless end-to-end and confirm frames arrive**

```bash
cargo build -p vantage-coordinator -p vantage-robot -p vantage-client
export RUST_LOG=info
BIN=target/debug
VANTAGE_BIND=127.0.0.1:8099 $BIN/vantage-coordinator >/tmp/p3_coord.log 2>&1 &
CP=$!
for i in $(seq 1 40); do curl -sf http://127.0.0.1:8099/healthz >/dev/null 2>&1 && break; sleep 0.25; done
VANTAGE_COORDINATOR=ws://127.0.0.1:8099 $BIN/vantage-robot >/tmp/p3_robot.log 2>&1 &
RP=$!
sleep 2
VANTAGE_COORDINATOR=ws://127.0.0.1:8099 $BIN/vantage-client >/tmp/p3_client.log 2>&1 &
CLP=$!
for i in $(seq 1 60); do grep -q "video frame" /tmp/p3_client.log && break; sleep 0.5; done
echo "### client:"; grep -E "status|telemetry|video frame" /tmp/p3_client.log | head
kill $CLP $RP $CP 2>/dev/null; wait 2>/dev/null
```

Expected: client logs `status: connected`, `telemetry: ...`, and `video frame 640x480 (#1)` then `(#30)`, `(#60)`. This proves end-to-end: robot encodes a test pattern → WebRTC → client decodes → RGBA frames, **alongside** telemetry on the data channel.

- [ ] **Step 5: Commit**

```bash
git add vantage-client/src/session.rs vantage-client/src/main.rs
git commit -m "feat(client): reusable run_session + headless frame verification

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Slint UI — test pattern beside live telemetry

Add the Slint window (video `Image` + telemetry panel), run it on the main thread, and run the session on a background Tokio thread. `VANTAGE_HEADLESS=1` keeps the Task 3 logger path for display-less verification.

**Files:**
- Modify: `vantage-client/Cargo.toml`
- Create: `vantage-client/src/ui.rs`
- Modify: `vantage-client/src/main.rs`

- [ ] **Step 1: Add the Slint dependency**

In `vantage-client/Cargo.toml`, under `[dependencies]`:

```toml
slint = "1"
```

- [ ] **Step 2: Define the UI component and the Slint-backed `UiSink`**

Create `vantage-client/src/ui.rs`:

```rust
use std::sync::Arc;

use slint::{Image, Rgba8Pixel, SharedPixelBuffer, SharedString, Weak};
use vantage_protocol::telemetry::DeviceInfo;
use vantage_signalling::peer::VideoFrame;

use crate::session::UiSink;

slint::slint! {
    import { VerticalBox, HorizontalBox } from "std-widgets.slint";
    export component AppWindow inherits Window {
        in property <image> video-frame;
        in property <string> telemetry-text: "waiting for telemetry…";
        in property <string> status-text: "starting…";
        title: "Vantage";
        preferred-width: 960px;
        preferred-height: 540px;
        HorizontalBox {
            Image {
                source: root.video-frame;
                width: parent.width * 70%;
                image-fit: contain;
            }
            VerticalBox {
                Text { text: root.status-text; font-size: 14px; }
                Text { text: root.telemetry-text; font-size: 13px; wrap: word-wrap; }
            }
        }
    }
}

pub use AppWindow as Window;

/// Bridges session callbacks (any thread) onto the Slint event loop.
pub struct SlintSink {
    ui: Weak<AppWindow>,
}

impl SlintSink {
    pub fn new(ui: Weak<AppWindow>) -> Arc<Self> {
        Arc::new(Self { ui })
    }
}

impl UiSink for SlintSink {
    fn frame(&self, frame: VideoFrame) {
        let buf = SharedPixelBuffer::<Rgba8Pixel>::clone_from_slice(
            &frame.rgba, frame.width, frame.height);
        let image = Image::from_rgba8(buf);
        let _ = self.ui.upgrade_in_event_loop(move |ui| ui.set_video_frame(image));
    }
    fn telemetry(&self, info: &DeviceInfo) {
        let text: SharedString = format!(
            "CPU {:.1}%\nMem {}/{} MB\nTemps {}\nUptime {}s",
            info.cpu_percent, info.mem_used_mb, info.mem_total_mb, info.temps.len(), info.uptime_s
        ).into();
        let _ = self.ui.upgrade_in_event_loop(move |ui| ui.set_telemetry_text(text));
    }
    fn status(&self, text: &str) {
        let text: SharedString = text.to_string().into();
        let _ = self.ui.upgrade_in_event_loop(move |ui| ui.set_status_text(text));
    }
}
```

> **Slint API caveat:** `slint = "1"` resolves to the latest 1.x. The setters `set_video_frame`/`set_telemetry_text`/`set_status_text` are generated from the `in property` names (`video-frame` → `set_video_frame`). `upgrade_in_event_loop`, `SharedPixelBuffer::clone_from_slice`, `Image::from_rgba8`, and `Rgba8Pixel` are stable Slint APIs; if a name differs in your 1.x point release, check `https://docs.rs/slint`.

- [ ] **Step 3: Wire `main.rs` — Slint on the main thread, session on a worker thread**

Replace `vantage-client/src/main.rs`:

```rust
mod session;
mod ui;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use session::{run_session, UiSink};
use ui::{AppWindow, SlintSink};
use vantage_protocol::telemetry::DeviceInfo;
use vantage_signalling::peer::VideoFrame;

fn coordinator_url() -> String {
    std::env::var("VANTAGE_COORDINATOR").unwrap_or_else(|_| "ws://localhost:8080".into())
}

fn spawn_session(sink: Arc<dyn UiSink>) {
    let coord = coordinator_url();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        if let Err(e) = rt.block_on(run_session(coord, sink)) {
            tracing::error!("session ended: {e}");
        }
    });
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Headless mode for display-less verification (CI / sandboxes).
    if std::env::var("VANTAGE_HEADLESS").is_ok_and(|v| v != "0" && !v.is_empty()) {
        let sink = Arc::new(LogSink { frames: AtomicU64::new(0) });
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
        return rt.block_on(run_session(coordinator_url(), sink));
    }

    let ui = AppWindow::new()?;
    let sink = SlintSink::new(ui.as_weak());
    spawn_session(sink);
    ui.run()?;
    Ok(())
}

struct LogSink {
    frames: AtomicU64,
}
impl UiSink for LogSink {
    fn frame(&self, frame: VideoFrame) {
        let n = self.frames.fetch_add(1, Ordering::Relaxed) + 1;
        if n == 1 || n % 30 == 0 {
            tracing::info!("video frame {}x{} (#{n})", frame.width, frame.height);
        }
    }
    fn telemetry(&self, info: &DeviceInfo) {
        tracing::info!("telemetry: cpu={:.1}% mem={}/{}MB", info.cpu_percent, info.mem_used_mb, info.mem_total_mb);
    }
    fn status(&self, text: &str) { tracing::info!("status: {text}"); }
}
```

- [ ] **Step 4: Build**

Run: `cargo build -p vantage-client`
Expected: 0 errors. (Slint pulls a fair amount on first build.)

- [ ] **Step 5: Verify the headless path still works (no display)**

Re-run the Task 3 Step 4 end-to-end commands, but launch the client with `VANTAGE_HEADLESS=1`:
```bash
VANTAGE_HEADLESS=1 VANTAGE_COORDINATOR=ws://127.0.0.1:8099 $BIN/vantage-client >/tmp/p3_client.log 2>&1 &
```
Expected: same `video frame 640x480` + `telemetry` logs as Task 3. This confirms the Slint refactor didn't break the session/frame path.

- [ ] **Step 6: Commit**

```bash
git add vantage-client/Cargo.toml vantage-client/src/ui.rs vantage-client/src/main.rs Cargo.lock
git commit -m "feat(client): Slint UI — test pattern beside live telemetry

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Integration — windowed exit criteria

**Files:** none (verification only). Optionally record evidence under `docs/superpowers/plans/notes/`.

- [ ] **Step 1: Run all three on a machine with a display**

```bash
RUST_LOG=info cargo run -p vantage-coordinator &
RUST_LOG=info VANTAGE_COORDINATOR=ws://localhost:8080 cargo run -p vantage-robot &
RUST_LOG=info VANTAGE_COORDINATOR=ws://localhost:8080 cargo run -p vantage-client
```

- [ ] **Step 2: Confirm the exit criteria**

Expected: the `vantage-client` window opens and shows the **SMPTE colour-bar test pattern**, smoothly updating, with the **telemetry panel** beside it showing CPU / Mem / Temps / Uptime that refresh every second. Status reads `connected`.

- [ ] **Step 3: (If no display available) record the headless evidence instead**

If running where no display exists, capture the Task 4 Step 5 headless output (frames + telemetry flowing) as the milestone evidence and note that the windowed render is pending a display, mirroring how the foundation milestone recorded its evidence.

- [ ] **Step 4: Commit any evidence note**

```bash
git add docs/superpowers/plans/notes/
git commit -m "test: phase 3 video test-pattern evidence"
```

---

## Phase 3 exit criteria (gate before Phase 4)

- [ ] `cargo test --workspace` green, including `robot_offer_contains_video_mline`.
- [ ] Headless end-to-end: the client logs decoded `640x480` frames **and** telemetry simultaneously (robot test pattern → WebRTC → client decode).
- [ ] Windowed (display available): the SMPTE test pattern renders in the Slint window beside the live telemetry panel (video-streaming "One-way video over WebRTC", isolated from camera/driver variables).

Once green, Phase 4 swaps `videotestsrc` for the real `v4l2src`/`nvarguscamerasrc`, adds the `tee` with the raw `sensor_msgs/Image` + `camera_info` branch (all of `camera-sharing`), and the runtime encoder factory — building on this proven negotiate→decode→render path.

---

## Self-review

**Spec coverage:** video-streaming "One-way video over WebRTC" → Task 1 (send-only transceiver, `m=video` asserted) + Task 2 (client receive/decode) + Task 4 (render). The remaining video-streaming requirements (fan-out, immediate-startup, demand-driven, adaptive-bitrate) are explicitly Phase 5; camera-sharing is Phase 4. No in-scope requirement is left without a task.

**Placeholder scan:** every code step contains complete code; verification steps give exact commands and expected output. The two "binding caveat" notes point at concrete resolution sources (installed crate source / docs.rs) rather than leaving anything as TODO.

**Type consistency:** `Role::{Robot,Client}`, `VideoFrame { width: u32, height: u32, rgba: Vec<u8> }`, `Peer::new(&[IceServer], Role)`, `Peer::recv_frame(&self) -> Option<VideoFrame>`, the `UiSink` trait (`frame`/`telemetry`/`status`), `run_session(String, Arc<dyn UiSink>)`, and the Slint setters (`set_video_frame`/`set_telemetry_text`/`set_status_text` from `video-frame`/`telemetry-text`/`status-text`) are used identically across Tasks 1–4. `PeerEvent` gains `#[derive(Debug)]` (Task 1 Step 6) which the test relies on.

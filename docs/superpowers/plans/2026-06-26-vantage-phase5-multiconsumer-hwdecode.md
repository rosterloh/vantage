# Vantage Phase 5 — Multi-consumer fan-out, hardware decode, dynamic branches Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the robot from a single-client streamer into a mini-SFU: capture and **encode once**, then fan the encoded RTP out to one `webrtcbin` **per connected client**, adding and removing those per-consumer branches at runtime as clients connect/disconnect. Add client-side **hardware H.264 decode** with a software fallback, give every new viewer an **immediate keyframe**, and wire **`transport-cc` adaptive bitrate**.

**Architecture:** Today each `ClientConnected` builds a brand-new `Peer(Role::Robot)` — a *complete* pipeline that re-opens the camera and re-runs the encoder. That cannot satisfy the spec's "encoder count does not increase" and physically fails on the second client (the camera is already held). Phase 5 splits the robot media into:

- **One persistent capture/encode engine** (`RobotMedia`), created at robot startup and owning the single GStreamer pipeline: `source → I420 caps → tee`, with the tee feeding (a) the encode chain `queue(leaky) → encoder → h264parse → rtph264pay → rtpcaps → rtptee` and (b) the existing raw RGB branch (`queue → videoconvert → appsink → RawFrame`). The `rtptee` (a `tee` on `application/x-rtp`) is the encode-once fan-out point.
- **One `Consumer` per client session** — a `queue → webrtcbin` pair added to that *same* pipeline, its `queue` sink linked from a freshly requested `rtptee` src pad. Each `Consumer` runs its own SDP offer/answer + ICE (keyed by session) and its own telemetry data channel. Branches are added on `ClientConnected` and torn down on `ClientDisconnected` via blocking pad probes.

The **client** stays one `Peer(Role::Client)` per process; only its decode element changes — a runtime decoder factory mirroring the existing `encoder.rs`.

**Tech Stack:** Rust (workspace, edition 2024, rust 1.90), tokio, GStreamer 1.28 via `gstreamer-rs` 0.23 (`gstreamer-app`, `gstreamer-video`, `gstreamer-webrtc`), `webrtcbin`, RTP `tee`, blocking pad probes, `GstForceKeyUnit`, `transport-cc`/`rtpgccbwe`.

**Scope:** This is the spec's **Phase 5** (`openspec/changes/add-vantage-poc/tasks.md` §5) only. It delivers multi-consumer fan-out, hardware decode, demand-driven add/remove, keyframe-on-join, and adaptive bitrate. The following are **out of scope** and tracked separately (Phase 6 / carry-over debt), called out so nothing is silently assumed done:

- Fleet stats endpoints, mDNS LAN fast-path, and the teleop control channel + watchdog → **Phase 6** (`tasks.md` §6).
- Live WebRTC→`recv_raw_frame()` end-to-end **on real camera hardware** (4b's subscriber-delivery proof used synthetic frames) → carry-over from 4b; opportunistically re-checked in Task 5 but not a gate here.
- The Docker/CI two-lane harness deferred in 4b → carry-over; unchanged by this plan.

**Builds on:** Phase 4a (`docs/.../2026-06-19-vantage-phase4a-camera-tee-encoder.md`) and 4b (`...-phase4b-ros2-bridge.md`), both merged to `main`. The pre-encode `tee`, the `encoder` factory, `RawFrame`/`recv_raw_frame`, and the feature-gated ROS bridge all exist and are reused unchanged.

---

## Spec coverage (this plan)

- **video-streaming "Encode once, fan out to multiple consumers"** → Tasks 2, 3 (`RobotMedia` rtptee + per-consumer `webrtcbin`; encoder built exactly once). Verified Task 5 (two viewers, single `selected H.264 encoder` log line).
- **video-streaming "Demand-driven streaming"** → Task 3 (add on `ClientConnected`, blocking-pad-probe teardown on `ClientDisconnected`).
- **video-streaming "Immediate startup for new viewers"** → Task 4 (`GstForceKeyUnit` on join; periodic IDR backstop from `key-int-max=30` already set in 4a's encoder config).
- **video-streaming "Adaptive bitrate"** → Task 4 (`transport-cc` + `rtpgccbwe` → encoder bitrate). Structure on this host; full verification on a host with `rtpgccbwe` (see Prerequisites).
- **Project non-negotiable** "must not depend on a specific GPU vendor; hardware decode with software fallback" (client side mirror of 4a's encoder rule) → Task 1 (`decoder.rs`).
- **camera-sharing** "Camera not monopolised" remains satisfied — the raw branch is unchanged and now provably coexists with *multiple* consumers (Task 5).

---

## Prerequisites — what this box can and cannot verify

Probed on the dev host (`gst-inspect-1.0`):

| Element | Role | Present here? |
|---|---|---|
| `x264enc` | software encode (fallback) | ✅ |
| `avdec_h264` | software decode (fallback) | ✅ |
| `nvh264dec` / `nvdec` / `vah264dec` / `vaapih264dec` | hardware decode | ❌ (none) |
| `nvh264enc` / `vah264enc` | hardware encode | ❌ (none) |
| `rtpgccbwe` | GCC bandwidth estimator for adaptive bitrate | ❌ |

Consequences, stated honestly (this mirrors how 4a treated hardware encoders):

1. **Decoder factory (Task 1)** selects `avdec_h264` here. Hardware-decoder arms are written from GStreamer docs and exercised on a VAAPI/NVDEC host; the *selection + build + decode-to-RGBA* path is fully testable here via the software decoder.
2. **Adaptive bitrate (Task 4)** can be wired (TWCC header extension + `rtpgccbwe` attach + bitrate setter) but the **bandwidth-falls → bitrate-drops** scenario can only be observed on a host where `rtpgccbwe` exists (it ships in `gst-plugin-webrtc` / `gst-plugins-rs`). Where absent, Task 4 records the structure as built and the scenario as host-deferred — not silently skipped.
3. **Multi-consumer fan-out, dynamic add/remove, keyframe-on-join (Tasks 2,3,4)** are fully verifiable here with `x264enc` + `avdec_h264` and two headless clients.

> `rtpgccbwe` is worth installing on the dev host if possible (`gstreamer1.0-plugins-rs` / build `gst-plugins-rs` net/webrtc) so Task 4 is verifiable locally. If it cannot be installed, that is an environment limit, not a code defect.

---

## File Structure

| File | Responsibility | Tasks |
|------|----------------|-------|
| `vantage-signalling/src/decoder.rs` | **Create.** Runtime H.264 decoder selection (hw → `avdec_h264`), mirror of `encoder.rs`. Unit-tested. | 1 |
| `vantage-signalling/src/lib.rs` | **Modify.** `pub mod decoder; pub mod robot_media;` | 1, 2 |
| `vantage-signalling/src/peer.rs` | **Modify.** Use `decoder::make_h264_decoder()` in the client decode branch; make `parse_sdp`, `wire_on_negotiation_needed`, `wire_data_channel` `pub(crate)` for reuse; move robot-only media helpers (`build_source`, `link_tee`, raw-branch builder) into `robot_media.rs`; retire the `Role::Robot` media path. | 1, 2 |
| `vantage-signalling/src/robot_media.rs` | **Create.** `RobotMedia` (shared pipeline + rtptee + raw channel) and `Consumer` (per-session `webrtcbin` + dc + events); `add_consumer`/`remove_consumer`; keyframe + bitrate. | 2, 3, 4 |
| `vantage-robot/src/main.rs` | **Modify.** Build `RobotMedia` once; keep a `HashMap<SessionId, Consumer>`; route `Signal{from}` per session; fan telemetry to all consumers; drain the raw branch from `RobotMedia` (ROS bridge unchanged). | 2, 3 |
| `vantage-client/src/...` | **No change** — client decode swap lives in `peer.rs`. | — |
| `vantage-coordinator/*` | **No change expected** — `Sessions` already maps many sessions per robot and `RobotMsg::Signal{to}` already routes robot→client per session (verified in Task 2 Step 1). | — |
| `docs/superpowers/plans/notes/2026-06-26-phase5-exit.md` | **Create.** Exit evidence (two viewers, one encoder; instant join; teardown). | 5 |

---

## Design decisions (read before Task 2)

1. **Why a new `robot_media.rs` instead of extending `Peer`.** `Peer` owns *its own* `gst::Pipeline`. Multiple `webrtcbin`s fed by one RTP `tee` MUST live in the *same* pipeline (pads can't be linked across pipelines). So the robot cannot be "one `Peer` per consumer." `RobotMedia` owns the single pipeline; each `Consumer` adds its `webrtcbin` into it. `Peer` is left untouched for the **client** (one pipeline, one connection — correct as is).
2. **Reuse, don't duplicate, the signalling glue.** `parse_sdp`, `wire_on_negotiation_needed`, and `wire_data_channel` already encapsulate offer/answer/ICE/dc wiring. They become `pub(crate)` and `Consumer` calls them — the per-consumer negotiation is identical to today's robot path, just scoped to one branch.
3. **The robot is the offerer per consumer** (unchanged contract). Each `Consumer` creates a `telemetry` data channel + a sendonly video transceiver and emits an `Offer`; the client answers. Signalling is keyed by `SessionId` end to end (`RobotMsg::Signal{to}` / `ServerMsg::Signal{from}` already carry it).
4. **`Role::Robot` is retired from `Peer`.** Its media-building (`add_video_source`) moves to `RobotMedia`; the `robot_offer_contains_video_mline` test migrates to assert `RobotMedia::add_consumer` produces an `m=video` offer. This removes the now-dead second media path rather than leaving it to rot (Surgical Changes: the orphan is created by *this* change).

---

## Task 1: Runtime H.264 decoder factory (client)

The client mirror of 4a's encoder factory: pick the best available decoder at runtime, fall back to `avdec_h264`. Isolated and TDD-able; lands first because it's independent of the robot refactor.

**Files:**
- Create: `vantage-signalling/src/decoder.rs`
- Modify: `vantage-signalling/src/lib.rs`, `vantage-signalling/src/peer.rs`

- [ ] **Step 1: Write the decoder module with inline tests**

Create `vantage-signalling/src/decoder.rs` (structure copied from `encoder.rs`):

```rust
use anyhow::{Context, Result};
use gstreamer as gst;
use gstreamer::prelude::*;

/// Ordered H.264 decoder candidates: hardware first, software (`avdec_h264`) last.
/// nvh264dec / nvdec (NVIDIA) → vah264dec / vaapih264dec (Intel/AMD VAAPI) →
/// d3d11h264dec (Windows) → vtdec_h264 (macOS) → avdec_h264 (software fallback).
pub(crate) const CANDIDATES: &[&str] = &[
    "nvh264dec",
    "nvdec",
    "vah264dec",
    "vaapih264dec",
    "d3d11h264dec",
    "vtdec_h264",
    "avdec_h264",
];

/// The factory name of the decoder that would be selected on this host, if any.
pub fn selected_decoder_name() -> Option<&'static str> {
    CANDIDATES
        .iter()
        .find(|n| gst::ElementFactory::find(n).is_some())
        .copied()
}

/// Build the first available H.264 decoder. Output caps are normalized by a
/// downstream `videoconvert` to RGBA, so the contract to the appsink is identical
/// regardless of which decoder is selected. Some hardware decoders emit
/// vendor-surface caps (NVMM / VASurface); on such hosts a hardware colour
/// converter (`vapostproc` / `nvvideoconvert`) may be needed before `videoconvert`
/// — see peer.rs build_decode_branch note.
pub fn make_h264_decoder() -> Result<gst::Element> {
    let name = selected_decoder_name()
        .context("no H.264 decoder found (install gst-plugins-libav for avdec_h264)")?;
    let dec = gst::ElementFactory::make(name)
        .build()
        .with_context(|| format!("failed to build decoder {name}"))?;
    tracing::info!("selected H.264 decoder: {name}");
    Ok(dec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn factory_builds_an_available_decoder() {
        gst::init().unwrap();
        let dec = make_h264_decoder().expect("an H.264 decoder must be available");
        let factory_name = dec.factory().expect("element has a factory").name();
        assert!(
            CANDIDATES.contains(&factory_name.as_str()),
            "selected decoder {factory_name} not in the candidate list"
        );
    }

    #[test]
    fn selection_is_deterministic_and_present() {
        gst::init().unwrap();
        assert!(selected_decoder_name().is_some(), "expected at least avdec_h264");
    }
}
```

- [ ] **Step 2: Export the module** — in `vantage-signalling/src/lib.rs` add `pub mod decoder;`

- [ ] **Step 3: Run the tests** — `cargo test -p vantage-signalling decoder` → both pass (`avdec_h264` selected here).

- [ ] **Step 4: Use the factory in the client decode branch**

In `vantage-signalling/src/peer.rs`, `build_decode_branch` (currently line 454), replace:
```rust
    let dec = gst::ElementFactory::make("avdec_h264").build()?;
```
with:
```rust
    let dec = crate::decoder::make_h264_decoder()?;
```

> **Hardware-decoder caps note:** the existing branch is `depay → parse → dec → videoconvert → appsink(RGBA)`. `videoconvert` handles software `avdec_h264` output. If a *hardware* decoder emits vendor-surface memory (NVMM / `video/x-raw(memory:VASurface)`), insert the matching hardware converter (`vapostproc` for VAAPI, `nvvideoconvert` for NVIDIA) before `videoconvert`. Only `avdec_h264` is exercisable here; do not invent element names for decoders you cannot test. This is the decode-side twin of 4a's encoder caveat.

- [ ] **Step 5: Verify the client still decodes** — `cargo test -p vantage-signalling` (existing `peer_is_send_sync` etc. green) and `cargo build --workspace`.

- [ ] **Step 6: Commit**
```bash
git add vantage-signalling/src/decoder.rs vantage-signalling/src/lib.rs vantage-signalling/src/peer.rs
git commit -m "feat(signalling): runtime H.264 decoder factory (hw select, avdec_h264 fallback)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: `RobotMedia` — single shared capture/encode engine + first consumer

The keystone. Extract the persistent capture/encode pipeline into `RobotMedia`, insert the `rtptee` fan-out point, and define `Consumer` + `add_consumer` so exactly one consumer reproduces today's single-client behaviour (regression-safe before fan-out in Task 3).

**Files:**
- Create: `vantage-signalling/src/robot_media.rs`
- Modify: `vantage-signalling/src/lib.rs`, `vantage-signalling/src/peer.rs`, `vantage-robot/src/main.rs`

**Interfaces (relied on by Tasks 3–4 and the robot binary):**
- `pub struct RobotMedia` — owns the pipeline, the `rtptee` (`gst::Element`), and the raw-frame channel.
- `pub fn RobotMedia::new(ice_servers: &[IceServer]) -> Result<RobotMedia>` — builds + PLAYs the engine (camera, tee, encode chain, rtptee, raw branch). Encoder is built **once** here.
- `pub fn RobotMedia::add_consumer(&self, session: SessionId) -> Result<Consumer>` — request an `rtptee` src pad, add `queue → webrtcbin` for this session, create its dc + offer.
- `pub fn RobotMedia::remove_consumer(&self, consumer: Consumer)` — blocking-pad-probe teardown (filled in Task 3; in Task 2 it may be a simple immediate unlink+remove).
- `pub async fn RobotMedia::recv_raw_frame(&self) -> Option<RawFrame>` — the raw branch drain (moved from `Peer`; ROS bridge consumes it unchanged).
- `pub struct Consumer { pub session: SessionId, /* webrtcbin, dc, events */ }` with `recv_event`, `handle_signal`, `send_data` — same surface as today's `Peer` but scoped to one branch and reusing peer.rs's free functions.

- [ ] **Step 1: Confirm the coordinator already fans signalling per session (no-code check)**

Read `vantage-coordinator/src/routes.rs` + `sessions.rs`: `Sessions` maps many sessions→one robot; `ClientMsg::Connect` relays `ClientConnected{session}`; client `Signal` relays to the robot tagged `from: Some(session)`; `RobotMsg::Signal{to}` relays back to that client session. **Expectation:** no coordinator change is required for N clients on one robot. Record this; if Task 5 reveals a routing gap, fix it then and note the deviation.

- [ ] **Step 2: Make the shared signalling helpers reusable**

In `peer.rs`, change visibility from private to `pub(crate)` for: `parse_sdp`, `wire_on_negotiation_needed`, `wire_data_channel`, and the `PeerEvent` enum (already `pub`). Move `build_source`, `link_tee`, and the raw-RGB appsink builder out of `peer.rs` into `robot_media.rs` (they are robot-only). Delete `add_video_source` from `peer.rs` and the `Role::Robot` arm's call to it (the `Role::Robot` data-channel/offer wiring also moves conceptually into `Consumer`).

> Keep `Role` and `Peer::new(Role::Client)` exactly as they are — the client path is unaffected. If removing the `Role::Robot` arm leaves `Role::Robot` unconstructed, either keep the variant (used by `Consumer` internally to label intent) or drop it and update imports; pick whichever yields zero dead-code warnings.

- [ ] **Step 3: Implement `RobotMedia::new` (engine built once)**

Create `vantage-signalling/src/robot_media.rs`. The engine is the 4a graph with the encode chain terminating in an **`rtptee`** instead of a single `webrtcbin`:

```rust
//! Robot-side media engine: ONE pipeline that captures and H.264-encodes the
//! camera exactly once, then fans the encoded RTP out via `rtptee` to one
//! `webrtcbin` per connected client (`Consumer`). The raw pre-encode branch
//! (RawFrame) is unchanged from 4a and feeds the ROS bridge.

use anyhow::{Context, Result};
use gstreamer as gst;
use gstreamer::prelude::*;
use tokio::sync::mpsc;
use vantage_protocol::signalling::IceServer;
use vantage_protocol::SessionId;

use crate::peer::RawFrame; // reuse the existing type

pub struct RobotMedia {
    pipeline: gst::Pipeline,
    rtptee: gst::Element,
    ice_servers: Vec<IceServer>,
    raw_frames_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<RawFrame>>,
}

impl RobotMedia {
    pub fn new(ice_servers: &[IceServer]) -> Result<Self> {
        gst::init()?;
        let pipeline = gst::Pipeline::new();

        // source → videoconvert → I420 caps(640x480@30) → tee  (verbatim from 4a)
        let source = build_source()?;
        let srcconvert = gst::ElementFactory::make("videoconvert").build()?;
        let srccaps = gst::ElementFactory::make("capsfilter")
            .property("caps", &gst::Caps::builder("video/x-raw")
                .field("format", "I420")
                .field("width", 640i32).field("height", 480i32)
                .field("framerate", gst::Fraction::new(30, 1))
                .build())
            .build()?;
        let tee = gst::ElementFactory::make("tee").name("t").build()?;

        // encode branch: tee → queue(leaky) → encoder(ONCE) → h264parse → rtph264pay → rtpcaps → rtptee
        let equeue = gst::ElementFactory::make("queue")
            .property_from_str("leaky", "downstream").build()?;
        let enc = crate::encoder::make_h264_encoder()?; // built exactly once
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
        let rtptee = gst::ElementFactory::make("tee").name("rtptee")
            .property("allow-not-linked", true) // engine keeps running with zero consumers
            .build()?;

        // raw branch: tee → queue → videoconvert → RGB appsink → RawFrame  (verbatim from 4a)
        let (raw_tx, raw_frames_rx) = mpsc::unbounded_channel::<RawFrame>();
        let (rqueue, rconvert, rawsink_el) = build_raw_branch(&raw_tx)?;

        pipeline.add_many([&source, &srcconvert, &srccaps, &tee,
                           &equeue, &enc, &parse, &pay, &rtpcaps, &rtptee,
                           &rqueue, &rconvert, &rawsink_el])?;
        gst::Element::link_many([&source, &srcconvert, &srccaps, &tee])?;
        gst::Element::link_many([&equeue, &enc, &parse, &pay, &rtpcaps, &rtptee])?;
        link_tee(&tee, &equeue)?;
        gst::Element::link_many([&rqueue, &rconvert, &rawsink_el])?;
        link_tee(&tee, &rqueue)?;

        pipeline.set_state(gst::State::Playing)?;

        Ok(Self {
            pipeline,
            rtptee,
            ice_servers: ice_servers.to_vec(),
            raw_frames_rx: tokio::sync::Mutex::new(raw_frames_rx),
        })
    }

    pub async fn recv_raw_frame(&self) -> Option<RawFrame> {
        self.raw_frames_rx.lock().await.recv().await
    }
}
```

> `allow-not-linked=true` on `rtptee` lets the capture/encode keep flowing with zero consumers attached — tearing down capture/encode at zero consumers is an explicit *later* power optimization (design §8), not Phase 5.
>
> `build_source`, `link_tee`, and `build_raw_branch` are the helpers moved from `peer.rs` in Step 2 — paste them at the bottom of `robot_media.rs`. `build_raw_branch` returns the three `(queue, videoconvert, appsink-as-Element)` and installs the `new_sample` callback exactly as 4a's `add_video_source` did.

- [ ] **Step 4: Implement `Consumer` + `add_consumer`**

A consumer is one `webrtcbin` (configured with the same STUN/TURN logic as `Peer::new`) plus a `queue` tapped off `rtptee`, with its own events channel and telemetry data channel. Reuse the peer.rs helpers made `pub(crate)` in Step 2:

```rust
pub struct Consumer {
    pub session: SessionId,
    webrtcbin: gst::Element,
    queue: gst::Element,
    rtptee_pad: gst::Pad,
    events_rx: tokio::sync::Mutex<mpsc::UnboundedReceiver<crate::peer::PeerEvent>>,
    events_tx: mpsc::UnboundedSender<crate::peer::PeerEvent>,
    data_channel: std::sync::Mutex<Option<gstreamer_webrtc::WebRTCDataChannel>>,
}

impl RobotMedia {
    pub fn add_consumer(&self, session: SessionId) -> Result<Consumer> {
        use gstreamer_webrtc as gst_webrtc;

        let (tx, rx) = mpsc::unbounded_channel();

        let webrtcbin = gst::ElementFactory::make("webrtcbin")
            .name(format!("wrb-{}", session.0))
            .property("bundle-policy", gst_webrtc::WebRTCBundlePolicy::MaxBundle)
            .build()
            .context("webrtcbin missing — install gst-plugins-bad")?;
        configure_ice(&webrtcbin, &self.ice_servers); // STUN/TURN block lifted from Peer::new

        // local ICE → PeerEvent::LocalIce  (same closure as Peer::new)
        wire_local_ice(&webrtcbin, &tx);

        let queue = gst::ElementFactory::make("queue")
            .property_from_str("leaky", "downstream").build()?;

        self.pipeline.add_many([&queue, &webrtcbin])?;
        queue.sync_state_with_parent()?;
        webrtcbin.sync_state_with_parent()?;

        // queue → webrtcbin (sendonly video)
        let qsrc = queue.static_pad("src").context("queue has no src pad")?;
        let wsink = webrtcbin.request_pad_simple("sink_%u")
            .context("webrtcbin refused a sink pad")?;
        qsrc.link(&wsink)?;
        let transceiver = webrtcbin
            .emit_by_name::<gst_webrtc::WebRTCRTPTransceiver>("get-transceiver", &[&0i32]);
        transceiver.set_property("direction", gst_webrtc::WebRTCRTPTransceiverDirection::Sendonly);

        // rtptee → queue  (the encode-once tap for THIS consumer)
        let rtptee_pad = self.rtptee.request_pad_simple("src_%u")
            .context("rtptee has no src pad")?;
        let qsink = queue.static_pad("sink").context("queue has no sink pad")?;
        rtptee_pad.link(&qsink)?;

        // telemetry data channel + offer  (robot is offerer, reusing peer.rs glue)
        let dc = webrtcbin.emit_by_name_with_values(
            "create-data-channel",
            &["telemetry".to_value(), None::<gst::Structure>.to_value()],
        ).context("create-data-channel returned no value")?
         .get::<gst_webrtc::WebRTCDataChannel>()
         .context("create-data-channel returned null")?;
        crate::peer::wire_data_channel(&dc, &tx);
        crate::peer::wire_on_negotiation_needed(&webrtcbin, &tx);

        Ok(Consumer {
            session, webrtcbin, queue, rtptee_pad,
            events_rx: tokio::sync::Mutex::new(rx),
            events_tx: tx,
            data_channel: std::sync::Mutex::new(Some(dc)),
        })
    }
}
```

`Consumer` exposes the same async surface the robot loop already speaks (copy the bodies from `Peer`): `recv_event`, `handle_signal` (offer/answer/ice — only `Answer` + `Ice` actually arrive at the robot), and `send_data`. `configure_ice`, `wire_local_ice`, `wire_data_channel`, `wire_on_negotiation_needed` are the shared bits — factor `configure_ice`/`wire_local_ice` out of `Peer::new` as `pub(crate)` helpers so both call sites use one copy.

- [ ] **Step 5: Rewire `vantage-robot/src/main.rs` for one consumer (regression parity)**

Build `RobotMedia` once at startup; on `ClientConnected` create a single `Consumer`; route events/signals through it. Keep a `HashMap<SessionId, Consumer>` from the start (Task 3 just stops it being size-1). The raw-branch drain now reads from `RobotMedia::recv_raw_frame` (the ROS bridge block is otherwise **unchanged** from 4b — it just consumes `media.recv_raw_frame()` instead of `peer.recv_raw_frame()`):

```rust
let media = Arc::new(RobotMedia::new(&ice)?);
let mut consumers: HashMap<SessionId, Consumer> = HashMap::new();

// raw drain (ROS bridge unchanged from 4b; reads from `media`)
spawn_raw_drain(media.clone() /*, ros_bridge */);
```

On `ClientConnected{session}`: `let c = media.add_consumer(session.clone())?; consumers.insert(session, c);`
On `ServerMsg::Signal{from: Some(session), signal}`: `if let Some(c) = consumers.get(&session) { c.handle_signal(signal)?; }`
For `PeerEvent::LocalDescription/LocalIce` from consumer *c*: send `RobotMsg::Signal { to: c.session.clone(), signal }`.
For telemetry tick: iterate `consumers.values()` whose dc is open and `send_data`.

> The event loop must now poll **all** consumers' `recv_event` (not a single `peer`). Use `futures::stream::SelectAll` over the consumers, or a small `tokio::select!` fed by a merged channel: give every `Consumer` the *same* `events_tx` cloned into a shared `mpsc` tagged with `SessionId`, so the loop selects one stream. The shared-channel approach is simpler — have `add_consumer` take an `mpsc::UnboundedSender<(SessionId, PeerEvent)>` and forward. Choose the shared-channel design; note it in the commit.

- [ ] **Step 6: Migrate the offer test + verify single-client parity**

Move `robot_offer_contains_video_mline` from `peer.rs` into `robot_media.rs` as: build `RobotMedia::new`, `add_consumer("test")`, await its first event, assert the `Offer` SDP contains `m=video`.

Run: `cargo test --workspace` → green (including the migrated test + Task 1's decoder tests).
Run the 4a-style single-client headless harness (coordinator + robot + one client) → client decodes frames, robot logs `selected H.264 encoder: x264enc` **once**, raw branch still drains. This proves the refactor is behaviour-preserving for one client before fan-out.

- [ ] **Step 7: Commit**
```bash
git add vantage-signalling/src/robot_media.rs vantage-signalling/src/lib.rs \
        vantage-signalling/src/peer.rs vantage-robot/src/main.rs
git commit -m "refactor(signalling): RobotMedia engine + per-session Consumer (encode-once fan-out base)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Demand-driven add/remove — fan out to N consumers, tear down on disconnect

Make the consumer map actually hold N entries and remove branches cleanly when clients leave, using a blocking pad probe (the canonical GStreamer dynamic-unlink pattern).

**Files:**
- Modify: `vantage-signalling/src/robot_media.rs` (`remove_consumer`), `vantage-robot/src/main.rs` (`ClientDisconnected`)

- [ ] **Step 1: Implement `remove_consumer` via a blocking pad probe**

Block the `rtptee` src pad feeding this consumer, then unlink + remove + release on the probe callback (running once the pad is idle, so no buffer is mid-flight):

```rust
impl RobotMedia {
    pub fn remove_consumer(&self, consumer: Consumer) {
        let Consumer { webrtcbin, queue, rtptee_pad, .. } = consumer;
        let pipeline = self.pipeline.clone();
        let rtptee = self.rtptee.clone();

        rtptee_pad.clone().add_probe(gst::PadProbeType::IDLE, move |pad, _info| {
            // 1. unlink rtptee → queue
            if let Some(qsink) = queue.static_pad("sink") {
                let _ = pad.unlink(&qsink);
            }
            // 2. tear the consumer elements out of the live pipeline
            let _ = pipeline.remove_many([&queue, &webrtcbin]);
            let _ = queue.set_state(gst::State::Null);
            let _ = webrtcbin.set_state(gst::State::Null);
            // 3. release the request pad so rtptee stops producing for it
            rtptee.release_request_pad(pad);
            gst::PadProbeReturn::Remove
        });
    }
}
```

> The IDLE probe fires when the pad is not passing a buffer, guaranteeing the unlink/remove is safe on a PLAYING pipeline. `allow-not-linked=true` (Task 2) means removing the *last* consumer does not error the engine. Removed `webrtcbin` going to `Null` closes its ICE/DTLS cleanly.

- [ ] **Step 2: Wire `ClientDisconnected` (and robot-side cleanup)**

In `main.rs`, the `ServerMsg::ClientDisconnected{session}` arm: `if let Some(c) = consumers.remove(&session) { media.remove_consumer(c); }`. (The old arm set `peer=None`; now it removes one entry from the map.) On coordinator-reported robot errors, drain the whole map.

- [ ] **Step 3: Two-client fan-out + teardown — headless**

Extend the 4a harness to start **two** clients against one robot:

```bash
export RUST_LOG=info; BIN=$(pwd)/target/debug
VANTAGE_BIND=127.0.0.1:8120 $BIN/vantage-coordinator >/tmp/p5_coord.log 2>&1 & CP=$!
for i in $(seq 1 40); do curl -sf http://127.0.0.1:8120/healthz >/dev/null 2>&1 && break; sleep 0.25; done
VANTAGE_COORDINATOR=ws://127.0.0.1:8120 $BIN/vantage-robot >/tmp/p5_robot.log 2>&1 & RP=$!
sleep 2
VANTAGE_HEADLESS=1 VANTAGE_COORDINATOR=ws://127.0.0.1:8120 $BIN/vantage-client >/tmp/p5_c1.log 2>&1 & C1=$!
sleep 2
VANTAGE_HEADLESS=1 VANTAGE_COORDINATOR=ws://127.0.0.1:8120 $BIN/vantage-client >/tmp/p5_c2.log 2>&1 & C2=$!
for i in $(seq 1 16); do sleep 0.5; done
echo "client1 frames: $(grep -c 'video frame' /tmp/p5_c1.log)"
echo "client2 frames: $(grep -c 'video frame' /tmp/p5_c2.log)"
echo "encoders built: $(grep -c 'selected H.264 encoder' /tmp/p5_robot.log)   # MUST be 1"
kill $C2; sleep 2                                  # client 2 leaves
echo "robot after c2 leave (teardown line):"; grep -iE 'remove|teardown|consumer' /tmp/p5_robot.log | tail -2
echo "client1 still streaming: keep observing"; sleep 3
echo "client1 frames (after c2 left): $(grep -c 'video frame' /tmp/p5_c1.log)"
kill $C1 $RP $CP 2>/dev/null; wait 2>/dev/null
```

Expected: **both** clients log video frames; `encoders built` == **1** (encode once — the spec's "encoder count does not increase"); after client 2 is killed the robot logs the consumer teardown and client 1 keeps receiving frames (no engine disruption). If client 2's frames stay 0, inspect `/tmp/p5_robot.log` for tee/pad link errors and report BLOCKED with the log.

- [ ] **Step 4: Commit**
```bash
git add vantage-signalling/src/robot_media.rs vantage-robot/src/main.rs
git commit -m "feat(signalling): demand-driven consumer add/remove via blocking pad probe

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Keyframe-on-join + `transport-cc` adaptive bitrate

Two independent stream-quality features. Keyframe-on-join is fully verifiable here; adaptive bitrate is wired here and verified on a host with `rtpgccbwe`.

**Files:**
- Modify: `vantage-signalling/src/robot_media.rs`

- [ ] **Step 1: Force a keyframe when a consumer joins**

After `add_consumer` links the branch, send an upstream `GstForceKeyUnit` so the (shared) encoder emits an IDR the new viewer can decode immediately, rather than waiting up to 1 s for the periodic IDR (`key-int-max=30` @30fps, the backstop):

```rust
// at the end of add_consumer, after the rtptee→queue link:
let fku = gstreamer_video::UpstreamForceKeyUnitEvent::builder()
    .all_headers(true)
    .build();
// Send upstream from this consumer's tee pad; it propagates to the encoder.
if !rtptee_pad.send_event(fku) {
    tracing::warn!("force-key-unit not handled (new viewer waits for periodic IDR)");
}
```

> FKU plumbing is finicky and encoder-dependent. The event must reach the encoder travelling **upstream**. If sending on `rtptee_pad` is not honoured by `x264enc`, send it instead on the encoder's src pad (`enc.static_pad("src")`) — keep a handle to `enc` in `RobotMedia` for that. Verify by the log/behaviour in Step 3, and use whichever pad actually triggers the IDR. The periodic IDR guarantees eventual decode regardless; FKU only shortens time-to-first-frame.

- [ ] **Step 2: Wire `transport-cc` adaptive bitrate**

Enable TWCC feedback and drive the encoder bitrate from the GCC estimate:

1. **Advertise TWCC** on the payloader so the client returns transport-wide congestion control feedback. On `rtph264pay`, add the TWCC RTP header extension (gstreamer-rs: `rtph264pay`'s `extensions` / `add-extension`, or include `rtcp-fb-transport-cc` in the RTP caps). Confirm the negotiated SDP contains `transport-cc`.
2. **Attach `rtpgccbwe`** to `webrtcbin` via its `request-aux-sender` signal (the estimator lives on the send side). Connect to `rtpgccbwe`'s `notify::estimated-bitrate` and clamp+apply it to the shared encoder's `bitrate` property (units per encoder — `x264enc` is kbit/s, so divide the bits/s estimate by 1000).

```rust
// sketch — exact signal/property names verified against the installed gst version:
webrtcbin.connect("request-aux-sender", false, move |vals| {
    let gcc = gst::ElementFactory::make("rtpgccbwe").build().ok()?;
    let enc = enc.clone();
    gcc.connect("notify::estimated-bitrate", false, move |g| {
        let bps: u32 = g[0].get::<gst::Element>().ok()?.property("estimated-bitrate");
        enc.set_property("bitrate", (bps / 1000).clamp(300, 2500)); // kbit/s for x264enc
        None
    });
    Some(gcc.to_value())
});
```

> **`rtpgccbwe` is absent on this host** (see Prerequisites). Build this wiring guarded so its absence does not break the engine (`ElementFactory::make("rtpgccbwe")` returning `Err` → log once, skip adaptive bitrate, keep streaming at the static 1.5 Mbit/s). Verify the **scenario** (bandwidth falls → bitrate drops) only where `rtpgccbwe` exists. Where it does not, record "adaptive-bitrate wired; scenario host-deferred" in the exit note — do not claim the scenario passed.

- [ ] **Step 3: Verify keyframe-on-join (here) + bitrate wiring**

- Keyframe: in the two-client harness (Task 3), measure **time-to-first-frame** for the *second* client — it should render within a few frames of joining, not after a ~1 s wait. Log the first `video frame` timestamp relative to `Connected` for client 2.
- Bitrate: on an `rtpgccbwe`-capable host, run client 1 with simulated downstream loss/bandwidth cap (e.g. `tc`/`netem` on loopback, or `VANTAGE_FORCE_RELAY` through a throttled TURN) and confirm the robot logs the encoder bitrate dropping. Where unavailable, confirm the guarded path logs "rtpgccbwe absent — static bitrate" and streaming continues.

- [ ] **Step 4: Commit**
```bash
git add vantage-signalling/src/robot_media.rs
git commit -m "feat(signalling): keyframe-on-join (force-key-unit) + transport-cc adaptive bitrate

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Integration evidence

Prove the Phase 5 exit criteria end to end and capture the evidence note.

**Files:**
- Create: `docs/superpowers/plans/notes/2026-06-26-phase5-exit.md`

- [ ] **Step 1: Run the full multi-consumer scenario** — the Task 3 harness, capturing: two clients both decoding; `encoders built == 1`; client-2 instant join (Task 4 Step 3); client-2 teardown with client-1 unaffected; adaptive-bitrate result (verified or host-deferred).

- [ ] **Step 2: (Opportunistic) camera + raw coexistence with two viewers** — if a working camera is available, run with `VANTAGE_VIDEO_SOURCE=camera` and (optionally) `--features ros`, and confirm the raw branch / ROS topics still publish while **two** clients stream — extending 4b's single-stream coexistence proof. If no camera, record proven-via-test-source (the 4a/4b convention). This also nudges the carry-over 4b "live raw on hardware" item but does not gate Phase 5.

- [ ] **Step 3: Record exit evidence** — create `docs/superpowers/plans/notes/2026-06-26-phase5-exit.md` modelled on the 4a/4b exit notes: the harness output, the single-encoder proof, join latency for the second viewer, the teardown line, and the adaptive-bitrate status (with the `rtpgccbwe` caveat if host-deferred).

- [ ] **Step 4: Commit**
```bash
git add docs/superpowers/plans/notes/2026-06-26-phase5-exit.md
git commit -m "test: phase 5 exit evidence (two viewers, encode-once, instant join, teardown)"
```

---

## Phase 5 exit criteria (gate)

- [ ] `cargo test --workspace` green (decoder tests + migrated `RobotMedia` offer test + existing suites).
- [ ] **Encode once, fan out:** two clients view one robot simultaneously; the encoder is built/selected exactly once (`encoders built == 1`).
- [ ] **Immediate startup:** a second viewer joining mid-stream renders within a few frames (keyframe-on-join), not after the periodic IDR.
- [ ] **Demand-driven:** a viewer disconnecting tears down only its branch (blocking pad probe); other viewers and the engine are undisturbed.
- [ ] **Hardware decode:** the decoder factory selects a real decoder at runtime (`avdec_h264` here; VAAPI/NVDEC on a capable host) and the client still renders.
- [ ] **Adaptive bitrate:** `transport-cc` + `rtpgccbwe` wired; bitrate-falls scenario verified where `rtpgccbwe` exists, else recorded host-deferred with the guarded static-bitrate path proven.
- [ ] **Camera not monopolised** still holds with N>1 consumers (raw branch coexists).

Once green, **Phase 6** (`tasks.md` §6: fleet stats, mDNS LAN fast-path, teleop control channel + disconnect watchdog) is next.

---

## Self-review

**Spec coverage:** every `tasks.md` §5 bullet maps to a task — hardware decode → Task 1; encode-once fan-out → Tasks 2/3; demand-driven add/remove → Task 3; keyframe-on-join + adaptive bitrate → Task 4 — and each video-streaming requirement (encode-once, demand-driven, immediate-startup, adaptive-bitrate) is verified in Task 5. Phase 6 items (fleet stats, mDNS, teleop) and the 4b carry-over (live-raw-on-hardware, Docker/CI) are explicitly out of scope, not silently dropped.

**Architecture honesty:** the central claim — today's per-client `Peer` re-opens the camera and can't satisfy "encoder count does not increase" — is grounded in the current `vantage-robot/src/main.rs` (`Peer::new(Role::Robot)` per `ClientConnected`) and `peer.rs` (`add_video_source` builds source+encoder inside each `Peer`). The fix (one `RobotMedia` pipeline + `rtptee` + per-session `Consumer`) is mandated by design §8, not invented. The coordinator needs no change (verified in Task 2 Step 1: `Sessions` is already many-sessions-per-robot and `RobotMsg::Signal{to}` routes per session).

**Known-uncertain points (surfaced, not hidden):**
1. Hardware decoder element names + their surface-memory caps — Task 1 uses doc-sourced names, exercises only `avdec_h264` here, and flags the `vapostproc`/`nvvideoconvert` need for hw paths.
2. `GstForceKeyUnit` upstream pad placement — Task 4 Step 1 gives a primary pad and a concrete fallback, with the periodic IDR as the always-correct backstop.
3. `rtpgccbwe` is absent on this host — Task 4 Step 2 guards its absence and Task 5 records the bitrate scenario as host-deferred rather than claiming a pass.
4. Multi-consumer event-loop shape (select-all vs shared tagged channel) — Task 2 Step 5 commits to the shared `(SessionId, PeerEvent)` channel to keep the robot loop a single `select!`.

**Type/interface consistency:** `RobotMedia::{new, add_consumer, remove_consumer, recv_raw_frame}` and `Consumer::{session, recv_event, handle_signal, send_data}` are defined in Task 2 and consumed by Tasks 3–4 and `main.rs` with matching signatures. `decoder::{make_h264_decoder, selected_decoder_name, CANDIDATES}` mirrors `encoder`'s surface and is used at the single call site in `peer.rs::build_decode_branch`. `RawFrame` and the ROS bridge are reused unchanged from 4a/4b.
```

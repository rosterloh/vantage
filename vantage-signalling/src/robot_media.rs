//! Robot-side media engine: ONE pipeline that captures and H.264-encodes the
//! camera exactly once, then fans the encoded RTP out via `rtptee` to one
//! `webrtcbin` per connected client (`Consumer`). The raw pre-encode branch
//! (RawFrame) is unchanged from 4a and feeds the ROS bridge.

use anyhow::{Context, Result};
use gstreamer as gst;
use gstreamer::glib;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use gstreamer_rtp::prelude::*;
use gstreamer_video as gst_video;
use gstreamer_video::prelude::*;
use gstreamer_webrtc as gst_webrtc;
use tokio::sync::mpsc;
use vantage_protocol::SessionId;
use vantage_protocol::signalling::{IceServer, Signal};

use crate::peer::{
    PeerEvent, RawFrame, configure_ice, parse_sdp, wire_data_channel, wire_local_ice,
    wire_on_negotiation_needed,
};
use crate::tunnel::{self, SshTunnelConfig};

/// Live SSH tunnel tasks for one consumer; aborted when the consumer is removed.
type Tunnels = std::sync::Arc<std::sync::Mutex<tokio::task::JoinSet<()>>>;

/// The single shared capture/encode engine. Owns the pipeline, the `rtptee`
/// fan-out point, and the raw pre-encode frame channel. Built (and PLAYed) once
/// at robot startup; each connecting client adds a `Consumer` tapped off `rtptee`.
pub struct RobotMedia {
    pipeline: gst::Pipeline,
    rtptee: gst::Element,
    enc: gst::Element,
    ice_servers: Vec<IceServer>,
    raw_frames_rx: tokio::sync::Mutex<mpsc::Receiver<RawFrame>>,
    /// SSH tunnels; `None` (the default) closes every operator-created channel.
    ssh: Option<SshTunnelConfig>,
}

impl RobotMedia {
    pub fn new(ice_servers: &[IceServer]) -> Result<Self> {
        gst::init()?;
        let pipeline = gst::Pipeline::new();

        // source → videoconvert → I420 caps(640x480@30) → tee  (verbatim from 4a)
        let source = build_source()?;
        let srcconvert = gst::ElementFactory::make("videoconvert").build()?;
        let srccaps = gst::ElementFactory::make("capsfilter")
            .property(
                "caps",
                gst::Caps::builder("video/x-raw")
                    // Force I420 (4:2:0): cameras often negotiate YUYV (4:2:2), which
                    // x264enc baseline rejects. I420 also converts cleanly to RGB on
                    // the raw branch.
                    .field("format", "I420")
                    .field("width", 640i32)
                    .field("height", 480i32)
                    .field("framerate", gst::Fraction::new(30, 1))
                    .build(),
            )
            .build()?;
        let tee = gst::ElementFactory::make("tee").name("t").build()?;

        // encode branch: tee → queue(leaky) → encoder(ONCE) → h264parse → rtph264pay → rtpcaps → rtptee
        let equeue = gst::ElementFactory::make("queue")
            .property_from_str("leaky", "downstream")
            .build()?;
        let enc = crate::encoder::make_h264_encoder()?; // built exactly once
        let parse = gst::ElementFactory::make("h264parse").build()?;
        let pay = gst::ElementFactory::make("rtph264pay")
            .property("config-interval", -1i32)
            .property("pt", 96u32)
            .build()?;

        // Advertise transport-cc via RTP header extension so the SDP contains
        // `transport-cc` and the client returns TWCC feedback.
        // Set ext-id=3 (valid one-byte range 1-14) before add-extension; the
        // default u32::MAX fails the element's internal assertion.
        if let Ok(twcc_ext) = gst::ElementFactory::make("rtphdrexttwcc").build() {
            let ext = twcc_ext
                .clone()
                .dynamic_cast::<gstreamer_rtp::RTPHeaderExtension>()
                .expect("rtphdrexttwcc is an RTPHeaderExtension");
            ext.set_id(3);
            pay.emit_by_name::<()>("add-extension", &[&twcc_ext]);
            tracing::info!("transport-cc RTP header extension added to payloader (ext-id=3)");
        } else {
            tracing::warn!("rtphdrexttwcc not found — transport-cc disabled");
        }
        let rtpcaps = gst::ElementFactory::make("capsfilter")
            .property(
                "caps",
                gst::Caps::builder("application/x-rtp")
                    .field("media", "video")
                    .field("encoding-name", "H264")
                    .field("payload", 96i32)
                    .build(),
            )
            .build()?;
        let rtptee = gst::ElementFactory::make("tee")
            .name("rtptee")
            // engine keeps running with zero consumers attached
            .property("allow-not-linked", true)
            .build()?;

        // raw branch: tee → queue → videoconvert → RGB appsink → RawFrame  (verbatim from 4a)
        let (raw_tx, raw_frames_rx) = mpsc::channel::<RawFrame>(crate::peer::FRAME_QUEUE_DEPTH);
        let (rqueue, rconvert, rawsink_el) = build_raw_branch(&raw_tx)?;

        pipeline.add_many([
            &source,
            &srcconvert,
            &srccaps,
            &tee,
            &equeue,
            &enc,
            &parse,
            &pay,
            &rtpcaps,
            &rtptee,
            &rqueue,
            &rconvert,
            &rawsink_el,
        ])?;
        gst::Element::link_many([&source, &srcconvert, &srccaps, &tee])?;
        gst::Element::link_many([&equeue, &enc, &parse, &pay, &rtpcaps, &rtptee])?;
        link_tee(&tee, &equeue)?;
        gst::Element::link_many([&rqueue, &rconvert, &rawsink_el])?;
        link_tee(&tee, &rqueue)?;

        pipeline.set_state(gst::State::Playing)?;

        Ok(Self {
            pipeline,
            rtptee,
            enc,
            ice_servers: ice_servers.to_vec(),
            raw_frames_rx: tokio::sync::Mutex::new(raw_frames_rx),
            ssh: None,
        })
    }

    /// Accept operator-created `ssh` channels and bridge each to `cfg.target`.
    pub fn with_ssh(mut self, cfg: Option<SshTunnelConfig>) -> Self {
        self.ssh = cfg;
        self
    }

    /// Add a per-session consumer: request an `rtptee` src pad, add `queue → webrtcbin`
    /// for this session, create its telemetry data channel and trigger the offer.
    ///
    /// `events_tx` is the robot loop's shared, session-tagged channel; this consumer's
    /// `PeerEvent`s are forwarded onto it tagged with `session` so one loop can select
    /// over all consumers.
    pub fn add_consumer(
        &self,
        session: SessionId,
        events_tx: mpsc::UnboundedSender<(SessionId, PeerEvent)>,
    ) -> Result<Consumer> {
        // Per-consumer channel; a forwarder tags each event with the session id and
        // pushes it onto the shared loop channel.
        let (tx, mut rx) = mpsc::unbounded_channel::<PeerEvent>();
        let forwarder = {
            let events_tx = events_tx.clone();
            let session = session.clone();
            tokio::spawn(async move {
                while let Some(ev) = rx.recv().await {
                    if events_tx.send((session.clone(), ev)).is_err() {
                        break;
                    }
                }
            })
        };

        let webrtcbin = gst::ElementFactory::make("webrtcbin")
            // Unique per consumer: teardown runs asynchronously, so a reconnecting
            // session can add its new webrtcbin before the old one has left the bin.
            .name(format!("wrb-{}-{}", session.0, next_consumer_id()))
            .property("bundle-policy", gst_webrtc::WebRTCBundlePolicy::MaxBundle)
            .build()
            .context("webrtcbin missing — install gst-plugins-bad")?;
        configure_ice(&webrtcbin, &self.ice_servers);
        wire_local_ice(&webrtcbin, &tx);
        // Connected before the element goes PLAYING and gains its sink pad and data
        // channels: `on-negotiation-needed` can fire as soon as any of those happen,
        // and a consumer that misses it never offers (#49).
        wire_on_negotiation_needed(&webrtcbin, &tx);
        let queue = gst::ElementFactory::make("queue")
            .property_from_str("leaky", "downstream")
            .build()?;

        self.pipeline.add_many([&queue, &webrtcbin])?;
        queue.sync_state_with_parent()?;
        webrtcbin.sync_state_with_parent()?;

        // queue → webrtcbin (sendonly video)
        let qsrc = queue.static_pad("src").context("queue has no src pad")?;
        let wsink = webrtcbin
            .request_pad_simple("sink_%u")
            .context("webrtcbin refused a sink pad")?;
        qsrc.link(&wsink)?;
        let transceiver =
            webrtcbin.emit_by_name::<gst_webrtc::WebRTCRTPTransceiver>("get-transceiver", &[&0i32]);
        transceiver.set_property(
            "direction",
            gst_webrtc::WebRTCRTPTransceiverDirection::Sendonly,
        );

        // rtptee → queue  (the encode-once tap for THIS consumer)
        let rtptee_pad = self
            .rtptee
            .request_pad_simple("src_%u")
            .context("rtptee has no src pad")?;
        let qsink = queue.static_pad("sink").context("queue has no sink pad")?;
        rtptee_pad.link(&qsink)?;

        // transport-cc adaptive bitrate: attach rtpgccbwe as an aux sender so
        // the encoder bitrate tracks the GCC estimate.
        //
        // GUARD: check for factory availability BEFORE connecting the signal.
        // request-aux-sender REQUIRES a non-None GstElement return; returning None
        // panics in GLib's closure marshaller. So we only connect when the factory
        // is confirmed present. On this host rtpgccbwe is absent → log once, skip.
        if gst::ElementFactory::find("rtpgccbwe").is_some() {
            let enc_weak = self.enc.downgrade();
            let unit = self
                .enc
                .factory()
                .map(|f| crate::encoder::bitrate_unit(f.name().as_str()))
                .unwrap_or(crate::encoder::BitrateUnit::Kbps);
            webrtcbin.connect("request-aux-sender", false, move |_vals| {
                let gcc = match gst::ElementFactory::make("rtpgccbwe").build() {
                    Ok(g) => g,
                    Err(_) => {
                        // Factory was found but build failed — unexpected; log and
                        // fall through. We can't return None here, but this path is
                        // unreachable if the factory-find guard above passed.
                        tracing::error!("rtpgccbwe build failed after factory found");
                        return Some(
                            gst::ElementFactory::make("identity")
                                .build()
                                .unwrap()
                                .to_value(),
                        );
                    }
                };
                let enc_weak = enc_weak.clone();
                gcc.connect("notify::estimated-bitrate", false, move |args| {
                    let gcc_el = args[0].get::<gst::Element>().ok()?;
                    let bps: u32 = gcc_el.property("estimated-bitrate");
                    // Clamp to 300–2500 kbit/s, then scale to the encoder's unit.
                    let kbps = (bps / 1000).clamp(300, 2500);
                    let value = match unit {
                        crate::encoder::BitrateUnit::Kbps => kbps,
                        crate::encoder::BitrateUnit::Bps => kbps.saturating_mul(1000),
                    };
                    if let Some(enc) = enc_weak.upgrade() {
                        enc.set_property("bitrate", value);
                        tracing::debug!(
                            "adaptive bitrate: {value} ({unit:?}) from estimate {bps} bit/s"
                        );
                    }
                    None
                });
                tracing::info!("rtpgccbwe attached — adaptive bitrate active");
                Some(gcc.to_value())
            });
        } else {
            tracing::warn!("rtpgccbwe absent — static bitrate (adaptive bitrate host-deferred)");
        }

        // telemetry data channel + offer  (robot is offerer, reusing peer.rs glue)
        let dc = webrtcbin
            .emit_by_name_with_values(
                "create-data-channel",
                &["telemetry".to_value(), None::<gst::Structure>.to_value()],
            )
            .context("create-data-channel returned no value")?
            .get::<gst_webrtc::WebRTCDataChannel>()
            .context("create-data-channel returned null")?;
        wire_data_channel(&dc, &tx);

        // Reserve the operator→robot control channel in the SAME negotiation (day-one
        // bidirectional, no renegotiation). Unreliable/unordered (latest-command-wins);
        // inbound teleop arrives as PeerEvent::Control and is gated by the watchdog.
        let control_dc = webrtcbin
            .emit_by_name_with_values(
                "create-data-channel",
                &[
                    vantage_protocol::CONTROL_LABEL.to_value(),
                    control_dc_options().to_value(),
                ],
            )
            .context("create-data-channel(control) returned no value")?
            .get::<gst_webrtc::WebRTCDataChannel>()
            .context("create-data-channel(control) returned null")?;
        wire_control_channel(&control_dc, &tx);

        // Operator-created channels (SSH tunnels) arrive here; anything else is closed.
        let tunnels = Tunnels::default();
        wire_operator_channels(&webrtcbin, &session, self.ssh.clone(), &tunnels);

        // Force a keyframe so the new viewer gets an IDR immediately instead of
        // waiting up to ~1 s for the periodic IDR (key-int-max=30 @30fps).
        // Send the event upstream from the encoder's src pad — more reliable than
        // the rtptee pad for x264enc, which handles it at its output.
        let fku = gstreamer_video::UpstreamForceKeyUnitEvent::builder()
            .all_headers(true)
            .build();
        let enc_src = self.enc.static_pad("src");
        let sent = enc_src.as_ref().is_some_and(|p| p.send_event(fku));
        if !sent {
            tracing::warn!("force-key-unit not handled (new viewer waits for periodic IDR)");
        } else {
            tracing::debug!("force-key-unit sent to encoder for new consumer {session}");
        }

        Ok(Consumer {
            session,
            webrtcbin,
            queue,
            rtptee_pad,
            data_channel: std::sync::Mutex::new(Some(dc)),
            forwarder,
            tunnels,
        })
    }

    /// Tear down a consumer via an IDLE pad probe: it fires when the `rtptee` src
    /// pad feeding this consumer is not mid-buffer, so the unlink is safe on a
    /// PLAYING pipeline. The state changes then run on their own thread, never on
    /// the caller's (the robot's event loop) or a streaming thread.
    ///
    /// `webrtcbin` goes to Null before `queue`: until the transport is up,
    /// `webrtcbin` blocks incoming buffers, so the queue's streaming thread can be
    /// parked mid-push holding its pad's stream lock. Stopping the queue first would
    /// wait on that lock forever (#49); deactivating `webrtcbin` first flushes the
    /// push and releases it.
    pub fn remove_consumer(&self, consumer: Consumer) {
        let Consumer {
            session,
            webrtcbin,
            queue,
            rtptee_pad,
            forwarder,
            tunnels,
            ..
        } = consumer;
        tracing::info!("removing consumer {session}");
        forwarder.abort();
        // Drop every SSH connection riding on this session.
        tunnels.lock().unwrap().abort_all();
        let pipeline = self.pipeline.clone();
        let rtptee = self.rtptee.clone();

        rtptee_pad
            .clone()
            .add_probe(gst::PadProbeType::IDLE, move |pad, _info| {
                tracing::debug!("consumer teardown probe fired");
                // 1. unlink rtptee → queue, and release the request pad so rtptee
                //    stops producing for it (allow-not-linked=true means zero
                //    consumers does not error the engine)
                if let Some(qsink) = queue.static_pad("sink") {
                    let _ = pad.unlink(&qsink);
                }
                rtptee.release_request_pad(pad);
                // 2. stop and remove the branch off-thread, webrtcbin first (see above)
                let (pipeline, queue, webrtcbin) =
                    (pipeline.clone(), queue.clone(), webrtcbin.clone());
                let session = session.clone();
                std::thread::spawn(move || {
                    let _ = webrtcbin.set_state(gst::State::Null);
                    let _ = queue.set_state(gst::State::Null);
                    let _ = pipeline.remove_many([&queue, &webrtcbin]);
                    tracing::debug!("consumer {session} torn down");
                });
                gst::PadProbeReturn::Remove
            });
    }

    /// Await the next raw camera frame (RGB888) from the pre-encode tee branch.
    pub async fn recv_raw_frame(&self) -> Option<RawFrame> {
        self.raw_frames_rx.lock().await.recv().await
    }

    /// Set the pipeline to Null, releasing the camera device and encoder threads.
    pub fn shutdown(&self) {
        let _ = self.pipeline.set_state(gst::State::Null);
    }
}

/// Monotonic suffix for consumer element names (see `add_consumer`).
fn next_consumer_id() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// One connected client: a `webrtcbin` fed by a `queue` tapped off `rtptee`, plus
/// the telemetry data channel. Scoped to a single `rtptee` branch; events flow
/// through the shared session-tagged channel installed in `add_consumer`.
pub struct Consumer {
    pub session: SessionId,
    webrtcbin: gst::Element,
    queue: gst::Element,
    rtptee_pad: gst::Pad,
    data_channel: std::sync::Mutex<Option<gst_webrtc::WebRTCDataChannel>>,
    forwarder: tokio::task::JoinHandle<()>,
    tunnels: Tunnels,
}

/// Options for the `control` data channel: unreliable + unordered so a lost or late
/// teleop command never head-of-line-blocks the next (latest-command-wins). Safety
/// comes from the robot's disconnect watchdog, not retransmission.
fn control_dc_options() -> gst::Structure {
    gst::Structure::builder("config")
        .field("ordered", false)
        .field("max-retransmits", 0i32)
        .build()
}

/// Forward inbound `control`-channel bytes as `PeerEvent::Control`. The robot both
/// creates and receives on this channel (DCEP channels are bidirectional).
fn wire_control_channel(dc: &gst_webrtc::WebRTCDataChannel, tx: &mpsc::UnboundedSender<PeerEvent>) {
    let tx = tx.clone();
    dc.connect("on-message-data", false, move |vals| {
        if let Ok(bytes) = vals[1].get::<glib::Bytes>() {
            let _ = tx.send(PeerEvent::Control(bytes.to_vec()));
        }
        None
    });
}

/// Handle channels the operator creates on this session. Only reliable, ordered
/// `ssh` channels are accepted, and only when SSH is enabled and the session is
/// under its channel cap; each one is bridged to the configured target (never a
/// destination the operator chooses). Everything else is closed.
fn wire_operator_channels(
    webrtcbin: &gst::Element,
    session: &SessionId,
    ssh: Option<SshTunnelConfig>,
    tunnels: &Tunnels,
) {
    // Captured now: `on-data-channel` fires on a GStreamer thread with no runtime.
    let rt = match tokio::runtime::Handle::try_current() {
        Ok(rt) => Some(rt),
        Err(_) if ssh.is_none() => None,
        Err(_) => {
            tracing::warn!("no tokio runtime for {session}; SSH tunnels unavailable");
            None
        }
    };
    let session = session.clone();
    let tunnels = tunnels.clone();
    webrtcbin.connect("on-data-channel", false, move |vals| {
        let dc = vals[1].get::<gst_webrtc::WebRTCDataChannel>().unwrap();
        let label = dc.label().map(|l| l.to_string()).unwrap_or_default();
        let (Some(cfg), Some(rt)) = (&ssh, &rt) else {
            tracing::warn!("closing operator channel '{label}' from {session}: SSH disabled");
            dc.close();
            return None;
        };
        if label != vantage_protocol::SSH_LABEL {
            tracing::warn!("closing unexpected operator channel '{label}' from {session}");
            dc.close();
            return None;
        }
        if !tunnel::is_reliable_ordered(&dc) {
            tracing::warn!("closing unreliable/unordered ssh channel from {session}");
            dc.close();
            return None;
        }
        let mut set = tunnels.lock().unwrap();
        while set.try_join_next().is_some() {}
        if set.len() >= cfg.max_channels {
            tracing::warn!(
                "closing ssh channel from {session}: limit of {} reached",
                cfg.max_channels
            );
            dc.close();
            return None;
        }
        let id = dc.id();
        let (stream, pump) = tunnel::attach(dc, rt);
        let target = cfg.target.clone();
        let session = session.clone();
        tracing::info!("ssh channel {id} opened by {session} -> {target}");
        set.spawn_on(
            async move {
                match tunnel::bridge_to_tcp(stream, &target).await {
                    Ok((sent, received)) => tracing::info!(
                        "ssh channel {id} for {session} closed ({sent} B to robot, {received} B to operator)"
                    ),
                    Err(e) => tracing::warn!("ssh channel {id} for {session} ended: {e}"),
                }
                let _ = pump.await;
            },
            rt,
        );
        None
    });
}

impl Consumer {
    /// Apply a Signal received from this client (only Answer + Ice arrive at the robot).
    pub fn handle_signal(&self, signal: Signal) -> Result<()> {
        match signal {
            Signal::Offer { sdp } => {
                // The robot is the offerer; an inbound Offer is unexpected but handled
                // for completeness (mirror of Peer::handle_signal).
                let desc = parse_sdp(&sdp, gst_webrtc::WebRTCSDPType::Offer)?;
                self.webrtcbin
                    .emit_by_name::<()>("set-remote-description", &[&desc, &None::<gst::Promise>]);
            }
            Signal::Answer { sdp } => {
                let desc = parse_sdp(&sdp, gst_webrtc::WebRTCSDPType::Answer)?;
                self.webrtcbin
                    .emit_by_name::<()>("set-remote-description", &[&desc, &None::<gst::Promise>]);
            }
            Signal::Ice {
                candidate,
                sdp_mline_index,
            } => {
                self.webrtcbin
                    .emit_by_name::<()>("add-ice-candidate", &[&sdp_mline_index, &candidate]);
            }
        }
        Ok(())
    }

    /// Send bytes on the telemetry data channel.
    pub fn send_data(&self, bytes: &[u8]) -> Result<()> {
        if let Some(dc) = self.data_channel.lock().unwrap().as_ref() {
            let glib_bytes = glib::Bytes::from(bytes);
            dc.emit_by_name::<()>("send-data", &[&glib_bytes]);
        }
        Ok(())
    }
}

/// Build the raw pre-encode branch: queue → videoconvert → RGB appsink emitting
/// `RawFrame`s on `raw_tx`. Returns the three elements (appsink upcast to Element)
/// so the caller can add and link them. Lifted verbatim from 4a's `add_video_source`.
fn build_raw_branch(
    raw_tx: &mpsc::Sender<RawFrame>,
) -> Result<(gst::Element, gst::Element, gst::Element)> {
    let rqueue = gst::ElementFactory::make("queue").build()?;
    let rconvert = gst::ElementFactory::make("videoconvert").build()?;
    let rawsink = gst_app::AppSink::builder()
        .caps(
            &gst::Caps::builder("video/x-raw")
                .field("format", "RGB")
                .build(),
        )
        .max_buffers(2)
        .drop(true)
        .build();
    {
        let raw_tx = raw_tx.clone();
        let dropped = std::sync::atomic::AtomicU64::new(0);
        rawsink.set_callbacks(
            gst_app::AppSinkCallbacks::builder()
                .new_sample(move |sink| {
                    let sample = sink.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                    let buffer = sample.buffer().ok_or(gst::FlowError::Error)?;
                    let caps = sample.caps().ok_or(gst::FlowError::Error)?;
                    let info =
                        gst_video::VideoInfo::from_caps(caps).map_err(|_| gst::FlowError::Error)?;
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
                    crate::peer::try_push_frame(
                        &raw_tx,
                        RawFrame {
                            width: width as u32,
                            height: height as u32,
                            encoding: "rgb8".into(),
                            data,
                        },
                        &dropped,
                    );
                    Ok(gst::FlowSuccess::Ok)
                })
                .build(),
        );
    }
    let rawsink_el: gst::Element = rawsink.upcast();
    Ok((rqueue, rconvert, rawsink_el))
}

/// Request a `src_%u` pad off the tee and link it to a downstream element's sink.
fn link_tee(tee: &gst::Element, downstream: &gst::Element) -> Result<()> {
    let tee_src = tee
        .request_pad_simple("src_%u")
        .context("tee has no src pad")?;
    let sink = downstream
        .static_pad("sink")
        .context("downstream has no sink pad")?;
    tee_src.link(&sink)?;
    Ok(())
}

/// `VANTAGE_VIDEO_SOURCE=camera` → v4l2src (`VANTAGE_CAMERA_DEVICE`, default /dev/video0);
/// `VANTAGE_VIDEO_SOURCE=libcamera` → libcamerasrc (Raspberry Pi CSI camera modules,
/// which the legacy V4L2 stack can't drive; `VANTAGE_LIBCAMERA_NAME` selects a camera
/// by name when more than one is attached, default picks the first detected camera);
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
    } else if kind == "libcamera" {
        let name = std::env::var("VANTAGE_LIBCAMERA_NAME").ok();
        tracing::info!(
            "video source: libcamera {}",
            name.as_deref().unwrap_or("(default)")
        );
        let mut builder = gst::ElementFactory::make("libcamerasrc");
        if let Some(name) = &name {
            builder = builder.property("camera-name", name.as_str());
        }
        builder.build().context(
            "libcamerasrc — Raspberry Pi CSI camera source (install gstreamer1.0-libcamera)",
        )
    } else {
        tracing::info!("video source: test pattern");
        gst::ElementFactory::make("videotestsrc")
            .property_from_str("pattern", "smpte")
            .property("is-live", true)
            .build()
            .context("videotestsrc")
    }
}

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

    #[tokio::test]
    async fn robot_offer_contains_video_mline() {
        let media = RobotMedia::new(&ice()).expect("robot media builds");
        let (tx, mut rx) = mpsc::unbounded_channel();
        let _consumer = media
            .add_consumer(SessionId("test".into()), tx)
            .expect("consumer builds");
        let ev = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .expect("offer produced within 5s");
        match ev {
            Some((_, PeerEvent::LocalDescription(Signal::Offer { sdp }))) => {
                assert!(
                    sdp.contains("m=video"),
                    "offer must contain a video m-line:\n{sdp}"
                );
            }
            other => panic!("expected an Offer, got {other:?}"),
        }
    }

    /// A consumer whose session never connects still gets encoded buffers pushed
    /// at its `webrtcbin`, which blocks them until the transport is up. Removing it
    /// must not wait on that blocked push (#49).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn remove_consumer_that_never_connected_does_not_hang() {
        let media = std::sync::Arc::new(RobotMedia::new(&[]).expect("robot media builds"));
        let baseline = media.pipeline.children().len();
        for i in 0..3 {
            let (tx, _rx) = mpsc::unbounded_channel();
            let consumer = media
                .add_consumer(SessionId(format!("stalled-{i}")), tx)
                .expect("consumer builds");
            // Let the encoder fill the queue and block inside webrtcbin.
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            // A plain thread, so a hang fails the test instead of wedging the runtime.
            let m = media.clone();
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                m.remove_consumer(consumer);
                let _ = done_tx.send(());
            });
            done_rx
                .recv_timeout(std::time::Duration::from_secs(5))
                .expect("remove_consumer returns within 5s");
            // The off-thread teardown must actually finish, not just be scheduled.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while media.pipeline.children().len() != baseline {
                assert!(
                    std::time::Instant::now() < deadline,
                    "consumer {i} branch still in the pipeline after 5s"
                );
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    }

    #[tokio::test]
    async fn add_consumer_twice_same_session_then_remove_is_clean() {
        let media = RobotMedia::new(&ice()).expect("robot media builds");
        let (tx, _rx) = mpsc::unbounded_channel();
        let session = SessionId("s".into());

        let c1 = media
            .add_consumer(session.clone(), tx.clone())
            .expect("first consumer builds");
        media.remove_consumer(c1);

        let c2 = media
            .add_consumer(session, tx)
            .expect("second consumer builds");
        media.remove_consumer(c2);
    }
}

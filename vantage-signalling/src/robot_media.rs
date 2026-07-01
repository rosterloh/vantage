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
use vantage_protocol::signalling::{IceServer, Signal};
use vantage_protocol::SessionId;

use crate::peer::{
    configure_ice, parse_sdp, wire_data_channel, wire_local_ice, wire_on_negotiation_needed,
    PeerEvent, RawFrame,
};

/// The single shared capture/encode engine. Owns the pipeline, the `rtptee`
/// fan-out point, and the raw pre-encode frame channel. Built (and PLAYed) once
/// at robot startup; each connecting client adds a `Consumer` tapped off `rtptee`.
pub struct RobotMedia {
    pipeline: gst::Pipeline,
    rtptee: gst::Element,
    enc: gst::Element,
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
            .property(
                "caps",
                &gst::Caps::builder("video/x-raw")
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
                &gst::Caps::builder("application/x-rtp")
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
        let (raw_tx, raw_frames_rx) = mpsc::unbounded_channel::<RawFrame>();
        let (rqueue, rconvert, rawsink_el) = build_raw_branch(&raw_tx)?;

        pipeline.add_many([
            &source, &srcconvert, &srccaps, &tee, &equeue, &enc, &parse, &pay, &rtpcaps, &rtptee,
            &rqueue, &rconvert, &rawsink_el,
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
        })
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
        {
            let events_tx = events_tx.clone();
            let session = session.clone();
            tokio::spawn(async move {
                while let Some(ev) = rx.recv().await {
                    if events_tx.send((session.clone(), ev)).is_err() {
                        break;
                    }
                }
            });
        }

        let webrtcbin = gst::ElementFactory::make("webrtcbin")
            .name(format!("wrb-{}", session.0))
            .property("bundle-policy", gst_webrtc::WebRTCBundlePolicy::MaxBundle)
            .build()
            .context("webrtcbin missing — install gst-plugins-bad")?;
        configure_ice(&webrtcbin, &self.ice_servers);
        wire_local_ice(&webrtcbin, &tx);

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
        let transceiver = webrtcbin
            .emit_by_name::<gst_webrtc::WebRTCRTPTransceiver>("get-transceiver", &[&0i32]);
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
            webrtcbin.connect("request-aux-sender", false, move |_vals| {
                let gcc = match gst::ElementFactory::make("rtpgccbwe").build() {
                    Ok(g) => g,
                    Err(_) => {
                        // Factory was found but build failed — unexpected; log and
                        // fall through. We can't return None here, but this path is
                        // unreachable if the factory-find guard above passed.
                        tracing::error!("rtpgccbwe build failed after factory found");
                        return Some(gst::ElementFactory::make("identity").build().unwrap().to_value());
                    }
                };
                let enc_weak = enc_weak.clone();
                gcc.connect("notify::estimated-bitrate", false, move |args| {
                    let gcc_el = args[0].get::<gst::Element>().ok()?;
                    let bps: u32 = gcc_el.property("estimated-bitrate");
                    // x264enc bitrate is in kbit/s; clamp to 300–2500 kbit/s.
                    let kbps = (bps / 1000).clamp(300, 2500);
                    if let Some(enc) = enc_weak.upgrade() {
                        enc.set_property("bitrate", kbps);
                        tracing::debug!(
                            "adaptive bitrate: {kbps} kbit/s (estimate {bps} bit/s)"
                        );
                    }
                    None
                });
                tracing::info!("rtpgccbwe attached — adaptive bitrate active");
                Some(gcc.to_value())
            });
        } else {
            tracing::warn!(
                "rtpgccbwe absent — static bitrate (adaptive bitrate host-deferred)"
            );
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

        wire_on_negotiation_needed(&webrtcbin, &tx);

        // Force a keyframe so the new viewer gets an IDR immediately instead of
        // waiting up to ~1 s for the periodic IDR (key-int-max=30 @30fps).
        // Send the event upstream from the encoder's src pad — more reliable than
        // the rtptee pad for x264enc, which handles it at its output.
        let fku = gstreamer_video::UpstreamForceKeyUnitEvent::builder()
            .all_headers(true)
            .build();
        let enc_src = self.enc.static_pad("src");
        let sent = enc_src
            .as_ref()
            .is_some_and(|p| p.send_event(fku));
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
        })
    }

    /// Tear down a consumer via a blocking IDLE pad probe: fires when the
    /// `rtptee` src pad feeding this consumer is not mid-buffer, guaranteeing
    /// the unlink/remove is safe on a PLAYING pipeline.
    pub fn remove_consumer(&self, consumer: Consumer) {
        let Consumer { session, webrtcbin, queue, rtptee_pad, .. } = consumer;
        tracing::info!("removing consumer {session}");
        let pipeline = self.pipeline.clone();
        let rtptee = self.rtptee.clone();

        rtptee_pad.clone().add_probe(gst::PadProbeType::IDLE, move |pad, _info| {
            // 1. unlink rtptee → queue
            if let Some(qsink) = queue.static_pad("sink") {
                let _ = pad.unlink(&qsink);
            }
            // 2. remove queue + webrtcbin from the live pipeline and set to Null
            let _ = pipeline.remove_many([&queue, &webrtcbin]);
            let _ = queue.set_state(gst::State::Null);
            let _ = webrtcbin.set_state(gst::State::Null);
            // 3. release the request pad so rtptee stops producing for it
            //    (allow-not-linked=true means zero consumers does not error the engine)
            rtptee.release_request_pad(pad);
            gst::PadProbeReturn::Remove
        });
    }

    /// Await the next raw camera frame (RGB888) from the pre-encode tee branch.
    pub async fn recv_raw_frame(&self) -> Option<RawFrame> {
        self.raw_frames_rx.lock().await.recv().await
    }
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
fn wire_control_channel(
    dc: &gst_webrtc::WebRTCDataChannel,
    tx: &mpsc::UnboundedSender<PeerEvent>,
) {
    let tx = tx.clone();
    dc.connect("on-message-data", false, move |vals| {
        if let Ok(bytes) = vals[1].get::<glib::Bytes>() {
            let _ = tx.send(PeerEvent::Control(bytes.to_vec()));
        }
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
    raw_tx: &mpsc::UnboundedSender<RawFrame>,
) -> Result<(gst::Element, gst::Element, gst::Element)> {
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
    Ok((rqueue, rconvert, rawsink_el))
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
}
